# cljdekiq

Pronounced "clide-kick" is a client and server implementation of Sidekiq. This library is intended to be used along side a ruby app. Meaning, you can enqueue and run jobs from clojure, run jobs enqueued by a ruby app, or enqueue Sidekiq jobs to your ruby app. It all works!


## Usage

Sidekiq in ruby is a wonderful piece of software. It starts with you defining a `perform` function and extending the `Sidekiq::Worker` class to create a job system around your function. Feels very clojure-y!

Cljdekiq does the same thing. First, define your function. And from here you can start a server and push work.

```clojure
(ns my.app
 (require [cljdekiq.core :as ck])

(defn send-email [user-id] ...)

(defn main []
 (def app (->> (ck/conn)
            (ck/register send-email)))

 (def stop (ck/run))

 (ck/perform-async app send-email "user-123")

 (stop))
```

It's also possible to override any of the job specific information in two ways. First, you can specify relevant custom options when you register the job with the server. Second, you can use the `worker` fn to encapulate your default options.

```clojure
;; Method 1...
(def app
 (->> (ck/conn)
  (ck/regiser send-email :as "SomeRubyWorker"
                         :queue :backoffice)))

;; or...

;; Method 2...
(def send-email-worker
 (ck/worker send-email :as "SomeRubyWorker"
                       :queue :backoffice
                       :retry 2)

(def app
 (->> (ck/conn)
  (ck/regiser send-email-worker)

  ;; And you can always override options on your workers
  ;; when you register them.
  (ck/regiser send-email-worker :as "SomethingElse")))

```

Options

- `:as` -- Override the ruby-style class name used when creating or listening for jobs. If not specified, the function's namespace and function name will be used to generate a ruby style constant. Example: `some-lib.core/my-func` would become `SomeLib::Core::MyFunc`. Anonymous functions fail to generate a consistent name, so an `:as` option is required for that use-case.
- `:queue` -- Override the default queue. It is literally `"default"` if not specified.
- `:retry` -- Override the number of retries. You can specify `false` or an `integer` here. By default a job will retry `25` times.


Similar to Sidekiq, you can either create a job that is meant to be run _now_ or _later_. Let's start with `perform-async`.

```clojure
(defn send-email [user-id] ...)
(def app (ck/conn))

;; Rely on defaults.
(ck/perform-async app send-email "user-123")

;; Or you can customize the class, queue, and retries using the
;; worker function.
(ck/perform-async app
 (ck/worker send-email :as "CoolClassWorker")
 "user-123")

;; And if you just want to kick off a job to the ruby app, you
;; can provide a string clas name instead.
(ck/perform-async app "MyRubyWorker" arg1 arg2)
```

To create a job to run at some later date, you can use `perform-in`. It works _almost_ the same way as `perform-async` but adds a new parameter where you may specify the number of seconds from now until the job should be enqueued for processing.

```clojure
(defn send-email [user-id] ...)
(def app (ck/conn))

(ck/perform-in app send-email 30 "user-456")

;; To make the "30" a little easier to read, you can use some
;; of the built-in helper methods.
(ck/perform-in app send-email (ck/seconds 30) "user-456")

(ck/perform-in app send-email (ck/hours 1) "user-456")

;; you can also specify a worker or use the worker function
;; in-line the same way you did with perform-async.
(ck/perform-in app
 (ck/worker send-email :queue :backoffice)
 (ck/hours 1)
 "user-456")

(def send-email-worker
 (ck/worker send-email :queue :backoffice))

(ck/perform-in app send-email-worker (ck/hours 1) "user-123")

;; And of course, you can specify a ruby class directly.
(ck/perform-in "SyncHubspotChangesWorker" (ck/minutes 1) "user-999")
```

The developer experience is great with Cljdekick. It's very easy to turn your plain old functions into a Sidekiq job. Running work together has never been easier!

Now let's look at configuring the server to connect to redis, starting it, and stopping it.

By default, cljdekiq will create a `carmine` redis pool pointed at localhost.

```clojure
;; Create a redis pool pointed at localhost
(ck/conn)

;; Specify a custom redis path.
;; First we'll copy/ paste the carmine getting started example
(defonce my-conn-pool (car/connection-pool {}))
(def my-conn-spec {:uri "redis://myuser:pass@my-redis-host:1234/"})
(def my-wcar-opts {:pool my-conn-pool, :spec my-conn-spec})

;; Cljdekiq abstracts the redis parts into its own defrecord, which
;; is very easy to create using your carmine pool.
(def queue (cr/->RedisQueue my-wcar-opts))

;; Then you can pass your new queue as an argument to the conn function.
(ck/conn queue)
```

When you `run` your app, you need to make sure you keep the returned value. Running the server will not block your application. Instead, it returns a stop function that you can call when you want cljdekiq to shut down. This make take several seconds since the server will attempt to wait for any running jobs to complete.

```clojure
;; Create your app.
(def app (->> (ck/conn) (ck/register send-email)))

;; Start the server.
(def stop (ck/run app))

;; You can perform any other code you want here!

;; When you're ready, stop the server.
(stop)
```

You should now have all the information you need to use this library.


## Maintainer's Brain

The API feels good to work with, so I don't expect major changes. But, I do think some of the function names could be improved. Like `(ck/run conn)` is good but doesn't fully convey that this is a server with background threads that you must stop later.

All to say, anticipate breaking changes, but expect them to be simple to fix.

### Tasks

1. Add sidekiq-rs optimizations
2. Middleware
3. Using `core.async` maybe
4. Sidekiq-rs crons
5. Dynamo/Firestore/Postgres backend

### TODO

1. Better error handling on json deserializing
2. Can we have shutdown support `(stop :timeout n-secs)`
3. Should we rotate the queue order on `brpop`?
4. How do folks use loggers with clojure?

## License

Copyright © 2025 Comfy Soft

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
