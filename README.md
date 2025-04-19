# konserve-dynamodb

A [DynamoDB](https://aws.amazon.com/dynamodb/) backend for [konserve](https://github.com/replikativ/konserve). 

## Usage

Add to your dependencies:

[![Clojars Project](http://clojars.org/io.replikativ/konserve-dynamodb/latest-version.svg)](http://clojars.org/io.replikativ/konserve-dynamodb)

### Example

For asynchronous execution take a look at the [konserve example](https://github.com/replikativ/konserve#asynchronous-execution).


``` clojure
(require '[konserve-dynamodb.core :refer [connect-store]]
         '[konserve.core :as k])

(def dynamodb-spec
  {:region "us-west-1"
   :table  "konserve-demo"
   })

(def store (connect-store dynamodb-spec :opts {:sync? true}))

(k/assoc-in store ["foo" :bar] {:foo "baz"} {:sync? true})
(k/get-in store ["foo"] nil {:sync? true})
(k/exists? store "foo" {:sync? true})

(k/assoc-in store [:bar] 42 {:sync? true})
(k/update-in store [:bar] inc {:sync? true})
(k/get-in store [:bar] nil {:sync? true})
(k/dissoc store :bar {:sync? true})

;; Multi-key atomic operations (limited to 100 items per transaction by DynamoDB)
(k/multi-assoc store {:user1 {:name "Alice"} 
                       :user2 {:name "Bob"}} 
                {:sync? true})

(k/append store :error-log {:type :horrible} {:sync? true})
(k/log store :error-log {:sync? true})

(let [ba (byte-array (* 10 1024 1024) (byte 42))]
  (time (k/bassoc store "banana" ba {:sync? true})))

(k/bassoc store :binbar (byte-array (range 10)) {:sync? true})
(k/bget store :binbar (fn [{:keys [input-stream]}]
                        (map byte (slurp input-stream)))
       {:sync? true})

```

Note that you do not need full DynamoDB rights if you manage the bucket outside, i.e.
create it before and delete it after usage form a privileged account. Connection
will otherwise create the table and it can be deleted by `delete-store`. You can activate
[Amazon X-Ray](https://aws.amazon.com/xray/) by setting `:x-ray?` to `true` in
the DynamoDB spec.

## Multi-key Operations

This backend supports atomic multi-key operations through the `multi-assoc` function, which allows you to update multiple keys in a single atomic transaction. This ensures that either all operations succeed or all fail (ACID guarantees).

``` clojure
;; Update multiple keys atomically in a single transaction
(k/multi-assoc store {:user1 {:name "Alice"} 
                      :user2 {:name "Bob"}} 
               {:sync? true})
```

The implementation uses DynamoDB's TransactWriteItems API to ensure atomicity. Note that DynamoDB has a limit of 100 items per transaction, so attempting to update more than 100 keys in a single operation will result in an error.

## Authentication

A [common
approach](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html)
to manage AWS credentials is to put them into the environment variables as
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to avoid storing them in plain
text or code files. Alternatively you can provide the credentials in the
`dynamodb-spec` as `:access-key` and `:secret`.

## License

Copyright © 2024 Christian Weilbach

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
