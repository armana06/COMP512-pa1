# TCP travel reservation system

This directory contains the TCP version of the travel reservation assignment.
It uses the existing text-command client and resource-manager implementation,
with a middleware between the client and the three resource managers.

## Big picture

```text
                         one persistent connection
Client  -------------------------------------------------> Middleware
  |                                                          |
  | types commands such as                                   | routes each
  | AddFlight,101,2,120                                       | request to an RM
  |                                                          |
  |                                              one connection per request
  |                                    +---------------------+---------------------+
  |                                    |                     |                     |
  |                                    v                     v                     v
  |                                Flight RM              Car RM               Room RM
  |                                port 3042              port 3042             port 3042
  |                                    |                     |                     |
  |                                    +---- typed response-+---------------------+
  |                                                          |
  +<----------------------- response ------------------------+
```

The three resource managers can use the same Java implementation. The
middleware decides which manager owns each resource type:

| Resource or operation | Destination |
| --- | --- |
| Flights: add, delete, query, price, reserve | Flight RM |
| Cars: add, delete, query, price, reserve | Car RM |
| Rooms: add, delete, query, price, reserve | Room RM |
| Customer creation/deletion and bill queries | All three RMs |
| Bundle | Middleware coordinates the required RMs |

## What happens for one command?

For example, enter `AddFlight,101,2,120` in the client:

1. `Client.start()` reads the line and parses it into command and argument
   strings. The existing console and command parsing remain the same as the
   RMI client.
2. `Client.execute()` calls `addFlight(101, 2, 120)` on its
   `IResourceManager` reference. In the TCP client, that reference is a
   `TcpResourceManagerClient`, not an RMI stub.
3. The proxy creates a `Request` with command `"addFlight"` and typed
   arguments `Integer(101)`, `Integer(2)`, and `Integer(120)`. Its shared
   `invoke()` method sends requests for all API operations using the same
   mechanism.
4. `TcpChannel` writes the `Request` as a Java-serialized object to the
   client's persistent socket connection to the middleware.
5. A middleware client-handler thread reads the request and dispatches it to
   the Flight RM. For this forwarded operation, the middleware opens a TCP
   connection, sends the same request object, waits for the reply on that
   connection, and closes it.
6. The RM handler passes the request to `ResourceManagerDispatcher`. The
   dispatcher maps the command to the existing `ResourceManager` method and
   takes the RM's state lock while the operation executes.
7. The RM sends back a `Response` containing the method's result, here
   `Boolean.TRUE`. The middleware forwards that result in its own response to
   the client.
8. The client proxy reads the response and returns the expected Java type.
   The existing command handler prints `Flight added`.

Each request on the client-to-middleware connection is answered before the
client sends its next request. The middleware-to-RM socket is separate for
each forwarded operation, so its response belongs to that operation without
request IDs.

## Message format

The shared wire messages are in `Shared/`:

- `Request` contains a command name and an `Object[]` of arguments. Values
  retain their types, such as `Integer`, `Boolean`, `String`, and
  `Vector<String>`.
- `Response` contains either an `Object result` or an error string. An error
  is distinct from a normal result such as `false`, `0`, or an empty string.
- `TcpChannel` owns the socket's object input/output streams and provides
  methods to send or receive requests and responses. Both ends create the
  output stream first and flush its header before creating the input stream;
  this avoids both peers waiting indefinitely for the other's stream header.

To add a client-callable method, add a proxy method that calls the shared
`invoke(command, args...)` function and add the corresponding command case to
the dispatcher. Middleware routing must also be extended if the method belongs
to an RM or requires coordination across RMs.

## Concurrency and shared state

The middleware accepts a socket and starts a separate handler thread for each
client connection. A handler can wait for an RM response without stopping the
middleware from accepting other clients. Each RM similarly handles each
incoming request connection in its own thread.

Each RM's `ResourceManagerDispatcher` uses a fair read/write lock:

- Queries take the read lock, so multiple read-only queries can run together.
- Mutations take the write lock, so a mutation's related reads and writes are
  protected as one operation. For example, a reservation checks inventory,
  decreases the available count, and updates the customer's reservation while
  holding the same lock.

This prevents concurrent reservations from selling more items than are
available. Different RMs have independent locks, so flight, car, and room
operations can still proceed independently. There is no global FIFO order
across different clients: concurrent operations on the same RM take effect in
the order in which they acquire that RM's lock.

## Customers, bills, and bundles

Customer data is replicated at all three RMs. The middleware coordinates
creation and deletion, and combines the per-RM bill sections for a customer.
The bill's total is the sum of the three RM totals.

A bundle is coordinated by the middleware because it may involve flights,
cars, and rooms:

1. Reserve each requested flight, then the optional car and room.
2. Keep track of each reservation that succeeds.
3. If a later reservation fails, cancel only the reservations made by this
   bundle, in reverse order, and return failure.

This is compensating rollback, not a distributed transaction. Each RM operation
is protected by its local lock, but other requests may observe intermediate
bundle state before a later step fails and rollback runs. If a cancellation
itself fails, the middleware reports the rollback error rather than claiming
that the bundle was fully undone.

## Build

Run these commands from the `TCP` directory:

```sh
make -C Server compile-server-tcp
make -C Client compile-client
```

The launch scripts also build the necessary classes before starting their
process. A JDK is required because the scripts call `javac` and `java`.

## Run on one machine

Use four different ports when running all processes on one machine, because
each server needs its own listening port. Start three terminals from the
`TCP/Server` directory:

```sh
./run_server.sh Flights 35431
./run_server.sh Cars 35432
./run_server.sh Rooms 35433
```

Start the middleware in another terminal from `TCP/Server`:

```sh
./run_middleware.sh localhost localhost localhost 35431 35432 35433 35434
```

Then start the client from `TCP/Client`:

```sh
./run_client.sh localhost 35434
```

The optional ports are ordered as flight RM, car RM, room RM, then middleware
listen port. When a port is omitted, it defaults to `3042`.

## Run on separate machines

Run each RM on its machine using the default port:

```sh
./run_server.sh Flights
./run_server.sh Cars
./run_server.sh Rooms
```

On the middleware machine, pass the three RM hostnames in flight, car, room
order:

```sh
./run_middleware.sh <flight_host> <car_host> <room_host>
```

On the client machine:

```sh
./run_client.sh <middleware_host>
```

The convenience launcher starts those four server processes in `tmux` over
SSH. Its arguments are the flight RM, car RM, room RM, and middleware
hostnames, in that order:

```sh
./run_servers.sh <flight_host> <car_host> <room_host> <middleware_host>
```

The launcher assumes the project is available at the same absolute path on
each remote machine and that SSH access, `tmux`, Java, and `javac` are set up.

## Main files

| File | Responsibility |
| --- | --- |
| `Client/Client/Client.java` | Reads and executes existing console commands |
| `Client/Client/TCPClient.java` | Connects the console client to the middleware |
| `Client/Client/TcpResourceManagerClient.java` | Implements `IResourceManager` over TCP |
| `Shared/Request.java` | Serializable request envelope |
| `Shared/Response.java` | Serializable result/error envelope |
| `Shared/TcpChannel.java` | Shared object-stream socket transport |
| `Server/Server/TCP/TCPMiddlewareServer.java` | Client listener, resource routing, customer and bundle coordination |
| `Server/Server/TCP/TCPResourceManagerServer.java` | RM listener and per-connection request handling |
| `Server/Server/TCP/ResourceManagerDispatcher.java` | Command dispatch and per-RM state locking |
| `Server/Server/Common/ResourceManager.java` | Existing inventory, customer, and reservation behavior |
