# cljdekiq

A Clojure library designed to ... well, that part is up to you.

## Usage

FIXME

### Tasks

1. Abstract redis
2. Add sidekiq-rs optimizations
3. Middleware
4. Using `core.async` maybe
5. Sidekiq-rs crons
6. Dynamo/Firestore/Postgres backend

### TODO

1. Better error handling on json deserializing
2. Can we have shutdown support `(stop :timeout n-secs)`
3. Should we rotate the queue order on `brpop`?
4. How do folks use loggers with clojure?

## License

Copyright © 2025 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
