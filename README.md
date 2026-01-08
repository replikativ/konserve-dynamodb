# konserve-dynamodb

A [DynamoDB](https://aws.amazon.com/dynamodb/) backend for [konserve](https://github.com/replikativ/konserve). 

## Usage

Add to your dependencies:

[![Clojars Project](http://clojars.org/io.replikativ/konserve-dynamodb/latest-version.svg)](http://clojars.org/io.replikativ/konserve-dynamodb)

### Configuration

``` clojure
(require '[konserve-dynamodb.core]  ;; Registers the :dynamodb backend
         '[konserve.core :as k])

(def config
  {:backend :dynamodb
   :region "us-west-1"
   :table "konserve-demo"
   :id #uuid "550e8400-e29b-41d4-a716-446655440000"
   ;; Optional:
   :access-key "your-access-key"
   :secret "your-secret"
   :consistent-read? false  ;; Default: eventual consistency
   :x-ray? false})          ;; Enable AWS X-Ray tracing

(def store (k/create-store config {:sync? true}))
```

For API usage (assoc-in, get-in, delete-store, etc.), see the [konserve documentation](https://github.com/replikativ/konserve).

## Implementation Details

### Multi-key Operations

This backend supports atomic multi-key operations (`multi-assoc`, `multi-get`, `multi-dissoc`).

**⚠️ Important: All multi-key operations are limited to 100 items** due to DynamoDB API constraints. Exceeding this limit will throw an error.

| Operation | DynamoDB API | Atomicity | Limit |
|-----------|--------------|-----------|-------|
| `multi-assoc` | TransactWriteItems | Atomic (all-or-nothing) | 100 items |
| `multi-get` | BatchGetItem | Eventual/Strong consistency* | 100 items |
| `multi-dissoc` | TransactWriteItems | Atomic (all-or-nothing) | 100 items |

*Consistency for reads depends on the `:consistent-read?` option (default: `false` for eventual consistency).

## Authentication

A [common
approach](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html)
to manage AWS credentials is to put them into the environment variables as
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to avoid storing them in plain
text or code files. Alternatively you can provide the credentials in the
`dynamodb-spec` as `:access-key` and `:secret`.

## License

Copyright © 2024-2026 Christian Weilbach

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
