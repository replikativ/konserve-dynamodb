(ns konserve-dynamodb.layout
  "Physical layout for logical Konserve blobs. Fragments are immutable and never
   enumerated as logical keys. Old generations are retained for existing readers."
  (:import [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.util HashMap UUID Arrays]
           [software.amazon.awssdk.core SdkBytes]
           [software.amazon.awssdk.services.dynamodb.model AttributeValue]))

(def item-limit (* 400 1024))
(def transaction-limit (* 4 1024 1024))
(def fragment-size (* 300 1024))
(def fragment-prefix "konserve.fragment/")
(def format-name "konserve/chunked-v1")

(defn string-attr [^String x] (-> (AttributeValue/builder) (.s x) .build))
(defn binary-attr [^bytes x]
  (-> (AttributeValue/builder) (.b (SdkBytes/fromByteArray x)) .build))
(defn bytes-of [^AttributeValue x] (.asByteArray (.b x)))
(defn string-of [^AttributeValue x] (.s x))
(defn digest [^bytes x] (.digest (MessageDigest/getInstance "SHA-256") x))

(defn item-size
  "Exact size for the String/Binary attributes emitted by this layout. Manifest
   numeric fields are strings too, so no DynamoDB number-size estimate is needed."
  [item]
  (reduce-kv (fn [n ^String k ^AttributeValue v]
               (+ n (alength (.getBytes k StandardCharsets/UTF_8))
                  (if-let [s (.s v)]
                    (alength (.getBytes s StandardCharsets/UTF_8))
                    (.remaining (.asByteBuffer (.b v))))))
             0 (into {} item)))

(defn inline-item [key {:keys [header meta value]}]
  (HashMap. {"Key" (string-attr key)
             "Header" (binary-attr header) "Meta" (binary-attr meta)
             "Value" (binary-attr value)}))

(defn fragment-key [generation i] (str fragment-prefix generation "/" i))
(defn fragment-key? [^String key] (.startsWith key fragment-prefix))

(defn pack [{:keys [^bytes header ^bytes meta ^bytes value]}]
  (let [n (+ 12 (long (alength header)) (alength meta) (alength value))]
    (when (> n Integer/MAX_VALUE)
      (throw (ex-info "Logical blob exceeds JVM byte-array limit"
                      {:type :konserve.dynamodb/blob-too-large :bytes n})))
    (-> (ByteBuffer/allocate (int n))
        (.putInt (alength header)) (.putInt (alength meta)) (.putInt (alength value))
        (.put header) (.put meta) (.put value) .array)))

(defn unpack [^bytes payload]
  (let [buf (ByteBuffer/wrap payload)
        lengths [(.getInt buf) (.getInt buf) (.getInt buf)]]
    (when-not (and (every? #(>= % 0) lengths) (= (reduce + lengths) (.remaining buf)))
      (throw (ex-info "Invalid fragment envelope"
                      {:type :konserve.dynamodb/corrupt-blob})))
    (zipmap ["Header" "Meta" "Value"]
            (mapv (fn [n] (binary-attr (let [out (byte-array n)] (.get buf out) out))) lengths))))

(defn plan
  "Plan a write without I/O. A batch can lower inline-budget so its published
   items fit in one transaction even when all original values fit individually."
  ([key data inline-budget] (plan key data inline-budget true))
  ([key data inline-budget overflow-write?]
   (let [inline (inline-item key data)]
     (if (<= (item-size inline) inline-budget)
       {:item inline :fragments []}
       (let [_ (when-not overflow-write?
                 (throw (ex-info "Overflow writes are disabled; upgrade readers before enabling :overflow-write?"
                                 {:type :konserve.dynamodb/overflow-disabled
                                  :key key :bytes (item-size inline) :limit inline-budget})))
             payload (pack data)
             length (alength ^bytes payload)
             generation (str (UUID/randomUUID))
             n (quot (+ length (dec fragment-size)) fragment-size)
             item (HashMap. {"Key" (string-attr key)
                             "Format" (string-attr format-name)
                             "Generation" (string-attr generation)
                             "Chunks" (string-attr (str n))
                             "Length" (string-attr (str length))
                             "Digest" (binary-attr (digest payload))
                             ;; CAS token: normal items compare serialized Meta;
                             ;; manifests compare its digest. Hydration remembers
                             ;; the physical token, separate from decoded metadata.
                             "Meta" (binary-attr (digest (:meta data)))})]
         (when (> (item-size item) inline-budget)
           (throw (ex-info "Manifest exceeds item budget"
                           {:type :konserve.dynamodb/item-too-large
                            :key key :bytes (item-size item) :limit inline-budget})))
         {:item item
          :fragments (map (fn [i]
                            (HashMap. {"Key" (string-attr (fragment-key generation i))
                                       "Fragment" (binary-attr
                                                   (Arrays/copyOfRange ^bytes payload
                                                                       (int (* i fragment-size))
                                                                       (int (min length (* (inc i) fragment-size)))))}))
                          (range n))})))))

(defn manifest [item]
  (when (contains? item "Format")
    (try
      (let [format (string-of (get item "Format"))
            generation (string-of (get item "Generation"))
            n (Long/parseLong (string-of (get item "Chunks")))
            length (Long/parseLong (string-of (get item "Length")))
            hash (bytes-of (get item "Digest"))]
        (when-not (and (= format format-name)
                       (= generation (str (UUID/fromString generation)))
                       (<= 12 length Integer/MAX_VALUE)
                       (= n (quot (+ length (dec fragment-size)) fragment-size))
                       (= 32 (alength ^bytes hash)))
          (throw (IllegalArgumentException. "Invalid manifest fields")))
        {:generation generation :chunks n :length length :digest hash})
      (catch Exception e
        (throw (ex-info "Invalid DynamoDB blob manifest"
                        {:type :konserve.dynamodb/corrupt-blob
                         :key (some-> (get item "Key") string-of)} e))))))
