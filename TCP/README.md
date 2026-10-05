# TCP Resource Manager: Implementation and Operations Guide

This document describes the TCP implementation in this directory at the level
needed to build, run, test, debug, and extend it. The assignment PDF is the
primary behavioral specification; the existing RMI implementation and shared
`IResourceManager` contract provide compatibility context. This guide documents
what the TCP code currently does, including important limits where it does not
provide stronger distributed-systems guarantees.

## Contents

- [Scope and design goals](#scope-and-design-goals)
- [Architecture](#architecture)
- [Request lifecycle](#request-lifecycle)
- [TCP wire protocol](#tcp-wire-protocol)
- [Concurrency and state ownership](#concurrency-and-state-ownership)
- [Customer replication and bills](#customer-replication-and-bills)
- [Bundle reservation and compensation](#bundle-reservation-and-compensation)
- [Supported API and routing](#supported-api-and-routing)
- [Build and run](#build-and-run)
- [Integration test guide](#integration-test-guide)
- [Errors, timeouts, and shutdown](#errors-timeouts-and-shutdown)
- [Operational and security considerations](#operational-and-security-considerations)
- [Extension guidance](#extension-guidance)
- [Source map](#source-map)

## Scope and design goals

The TCP system exposes the assignment's `IResourceManager` operations without
using Java RMI for transport. Its topology consists of one client-facing
Middleware and three backend ResourceManagers:

| Component | Owns inventory for | Default TCP port |
|---|---|---:|
| Flights ResourceManager | Flights | 3101 |
| Cars ResourceManager | Cars | 3102 |
| Rooms ResourceManager | Rooms | 3103 |
| Middleware | Request routing and cross-manager operations | 3042 |

The implementation is designed to:

1. Keep the familiar `IResourceManager` API at the client boundary.
2. Use an explicit, typed wire format rather than Java object serialization.
3. Let the Middleware continue accepting client requests while backend work is
   in progress.
4. Let each ResourceManager process multiple requests concurrently.
5. Correlate multiplexed backend requests and responses correctly.
6. Keep cross-manager operations, notably customers, bills, and bundles, in the
   Middleware rather than copying that orchestration into each backend.
7. Reuse the existing inventory and reservation model where practical.

The Middleware performs orchestration, not durable transaction coordination.
Data is held in memory by each ResourceManager. There is no persistence,
replication log, or automatic recovery after process loss.

## Architecture

```text
  Client process
  TCPClient
      |
      | one TCP connection per API invocation
      v
  Middleware :3042
      |                         |                         |
      | persistent connection   | persistent connection   | persistent connection
      v                         v                         v
  Flights RM :3101          Cars RM :3102             Rooms RM :3103
  Flight inventory           Car inventory             Room inventory
  Customer replica           Customer replica          Customer replica
```

### Client adapter

`TcpResourceManagerClient` implements `IResourceManager`. Its public methods
convert Java method calls to a method name plus typed arguments, send one request
to the Middleware, and convert the response value back to the declared Java
type. This lets the existing command-line `Client` use the TCP transport
without having to know about the wire protocol.

`TCPClient` is the executable entry point. It constructs the adapter, calls
`getName()` as a connection check, then launches the existing interactive
client command loop.

### Middleware

The Middleware accepts client sockets and reads one request from each. It routes
inventory methods to the relevant ResourceManager, coordinates methods that
span managers, and writes one response to the requesting client.

It creates one long-lived backend socket per ResourceManager. Each backend
connection has:

- A synchronized writer used to send multiple requests without interleaving
  their bytes.
- A dedicated reader thread that continuously reads responses.
- A correlation table keyed by Middleware-generated request ID.

Responses can arrive in a different order from requests. The request ID and
ResourceManager identity together tell the Middleware which pending operation
received a response.

### ResourceManagers

Each `TCPResourceManagerServer` wraps one shared `ResourceManager` instance.
The server accepts backend connections, reads requests from each connection,
and submits each request to a fixed-size request pool. Responses are synchronized
per output stream, so concurrent workers cannot interleave response bytes.

The dispatcher explicitly maps supported method names and argument types to
the corresponding `ResourceManager` method. It does not use reflection.

### Shared model and RMI relationship

The TCP implementation reuses the shared `ResourceManager`, data classes, and
`IResourceManager` API used by the RMI implementation. The TCP-specific
transport and orchestration live under `Server/TCP` and `Client/TCPClient.java`.
The RMI server/runtime is not started by the TCP launch scripts.

The shared API includes a `bundle` method, but the shared `ResourceManager`
does not implement bundle orchestration itself. The TCP Middleware implements
bundle behavior by issuing ordinary reservation calls and compensating them on
failure. This keeps cross-manager behavior in the layer that can see all three
ResourceManagers.

## Request lifecycle

### A regular inventory call

For a call such as `queryFlight(100)`:

1. The application calls `TcpResourceManagerClient.queryFlight`.
2. The client allocates a monotonically increasing client request ID.
3. The client opens a TCP socket to the Middleware, writes a protocol request,
   and waits for one response.
4. The Middleware reads the request and maps `queryFlight` to the Flights
   ResourceManager.
5. It allocates its own backend request ID, registers a `PendingCall`, then
   sends the request on the existing Flights connection.
6. The Flights request worker invokes the method through
   `ResourceManagerDispatcher`.
7. The ResourceManager writes a response containing the backend request ID.
8. The Flights response-reader thread finds the pending call and completes it.
9. The Middleware writes a response using the original client request ID and
   closes the client socket.
10. The client checks the response ID, converts the value to `int`, and returns
    it to the caller.

Client and backend request IDs are distinct namespaces. The Middleware maps
between them through its pending call and `ClientReply` objects.

### An operation spanning ResourceManagers

Methods such as `newCustomer`, `deleteCustomer`, and `queryCustomerInfo` need
results from more than one backend. The Middleware sends the same backend
request ID to all target managers, records which managers are expected to
respond, and waits until a response or connection-failure result has been
recorded for every target. The completion callback then applies the method's
combining rule.

For example, customer creation succeeds only if all three managers report
success. Bill lookup collects the three manager responses and combines their
reservation entries.

### Request identifiers and out-of-order completion

The Middleware's `pending` map is concurrent and indexed by a unique `long`
request ID. Each `PendingCall` remembers its expected ResourceManagers and
stores at most one response from each. It completes exactly once after all
expected responses have been recorded, removes itself from the map, and invokes
its callback outside the synchronized state update.

The client-facing protocol also includes a request ID. The current client opens
a separate connection per call and sends only one request on that connection;
the response ID is nevertheless checked to detect an invalid or mismatched
reply.

## TCP wire protocol

The format is implemented by `TcpProtocol`. It uses
`DataInputStream`/`DataOutputStream` primitives and `writeUTF` strings. There is
no Java serialization and no separate length-prefixed frame around each
request. The fields are read in order from the TCP byte stream.

### Request layout

| Order | Field | Encoding |
|---:|---|---|
| 1 | Request ID | 8-byte signed `long` (`writeLong`) |
| 2 | Method name | Java modified UTF-8 (`writeUTF`) |
| 3 | Argument count | 4-byte signed `int` |
| 4 | Arguments | Repeated typed values |

### Response layout

| Order | Field | Encoding |
|---:|---|---|
| 1 | Request ID | 8-byte signed `long` |
| 2 | Is error | 1-byte boolean |
| 3a | Error message | `writeUTF`, when Is error is true |
| 3b | Return value | Typed value, when Is error is false |

### Value tags

Each value begins with a one-byte tag:

| Tag | Meaning | Payload |
|---:|---|---|
| 0 | `null` | None |
| 1 | `Integer` | 4-byte signed `int` |
| 2 | `Boolean` | 1-byte boolean |
| 3 | `String` | `writeUTF` string |
| 4 | `Vector<String>` | 4-byte item count, followed by that many `writeUTF` strings |

The protocol deliberately supports a small set of value types. Adding an
unsupported Java object fails serialization with `IOException`; do not assume
arbitrary objects, maps, or nested vectors can be sent.

### Bounds and validation

- A request can contain at most 64 arguments.
- A string vector can contain at most 10,000 strings.
- Negative counts and counts above those limits are rejected while decoding.
- Unknown value tags are rejected.
- A vector containing a non-string value cannot be written.
- The dispatcher validates each method's exact argument count and expected
  Java argument types.
- Middleware-local methods perform their own arity and type checks.
- `writeUTF` imposes its own encoded-string length limit; unusually long method
  arguments or error messages can therefore fail to encode.

Malformed protocol input can fail before a usable request ID has been read. In
that case, the Middleware may use ID `0` for its error response if it can still
write to the socket. A normal well-formed request retains its supplied ID.

## Concurrency and state ownership

### Where state lives

Each ResourceManager holds an independent in-memory `RMHashMap`:

- Flights manager: flight inventory and a replica of every customer.
- Cars manager: car inventory and a replica of every customer.
- Rooms manager: room inventory and a replica of every customer.

Customer reservations are kept in the corresponding manager's customer record
alongside that manager's inventory. This means each manager can process a
reservation and update its local inventory/customer reservation record together.

The system does not share a Java heap between processes. The fact that customer
IDs and customer records are replicated is implemented by Middleware calls,
not shared memory.

### Threading model

**Middleware**

- One daemon acceptor thread accepts client connections.
- A cached thread pool reads and routes client requests.
- One daemon response-reader thread runs for each backend connection.
- Callback completion handles responses without blocking a request handler on a
  backend `read`.

**ResourceManager server**

- One daemon acceptor thread accepts backend connections.
- A cached thread pool serves connection read loops.
- A fixed-size pool executes requests. Its size is
  `max(4, min(16, availableProcessors()))`.
- Requests read from one socket are submitted to the shared request pool and
  can execute concurrently. As a result, request completion order is not
  guaranteed to match request arrival order.
- Responses written to a given connection are synchronized to preserve the
  byte stream.

### Inventory synchronization

The shared `ResourceManager` synchronizes inventory-changing operations such as
adding inventory, deleting items, reserving an item, creating/deleting
customers, and canceling a reservation. In particular, the reservation path
checks the customer, checks available count, updates the customer's reservation,
decrements availability, and increments reserved count while holding the
ResourceManager monitor. Concurrent reservations therefore cannot both consume
the same final unit.

The underlying map synchronizes its individual read/write/remove operations.
Queries return values from cloned model records. As with the original shared
model, the system is in-memory and synchronization is process-local; there is
no distributed lock or durable transaction manager.

### Ordering and consistency

Concurrent calls can arrive and complete in different orders. The request ID
ensures responses are delivered to the correct operation; it does not impose
global ordering among distinct API calls. Callers that need one operation to
observe another must wait for the first call to complete before issuing the
dependent call.

## Customer replication and bills

### Explicit customer IDs

`newCustomer(customerId)` is sent to all three ResourceManagers:

- If all three return `true`, the Middleware returns `true`.
- If one or more return `false`, the Middleware attempts to delete the
  customer from the managers that reported successful creation and returns
  `false` if cleanup succeeds.
- If a manager returns an error, the Middleware attempts cleanup on managers
  that reported successful creation, then returns an error.
- If cleanup itself fails, the Middleware reports that the operation failed
  and includes cleanup failure context where available.

This compensates partial creation, but is not a durable atomic transaction.
Process or network failure during the operation can still leave partial state.

### Generated customer IDs

`newCustomer()` is handled by the Middleware. It proposes sequential IDs
starting at 100000, attempts registration on all three managers, and returns an
ID only after all registrations succeed. If an ID is already present on any
manager, successfully created partial records are cleaned up and another ID is
tried.

The counter is local to the Middleware process and is reset when that process
restarts. Existing customer IDs are handled by retrying after duplicate
registration; there is no persistent counter or centralized durable ID
allocator.

### Deleting a customer

`deleteCustomer(customerId)` is sent to every ResourceManager. The shared
ResourceManager restores each reserved item's available count and decrements
its reserved count before removing the customer record. Middleware returns
`true` only when every manager reports success.

Since deletion is applied independently, a backend failure can leave the
replicas inconsistent. The boolean result communicates aggregate success but
does not automatically repair a failed replica.

### Querying customer information

Each manager returns a bill fragment based on the reservations it owns. The
Middleware:

1. Fails the query if any ResourceManager response is an error.
2. Returns an empty string if all managers report that the customer does not
   exist.
3. Uses one common `Bill for customer <id>` heading.
4. Appends non-empty reservation lines from each manager.
5. Adds the numeric `Total cost: $...` values reported by each manager and
   emits one aggregate total.

The shared `Customer.getBill()` format includes a line per reservation key,
quantity, stored price, and a total calculated as `quantity * price`. If a
customer has multiple reservations for the same key, the count is aggregated;
the stored price is the latest reservation price for that key, consistent with
the existing shared model.

## Bundle reservation and compensation

The TCP Middleware executes `bundle(customerId, flightNumbers, location, car,
room)` as a sequence of ordinary reservation operations:

1. Validate the five argument types and require at least one flight.
2. Parse each flight number string as an integer.
3. Build steps in the input order: each flight, then an optional car, then an
   optional room.
4. Dispatch one step and wait asynchronously for its response.
5. On success, remember the step and advance to the next.
6. If every step succeeds, return `true`.
7. If a step fails, cancel the completed reservations in reverse order.
8. If all compensation succeeds, return `false` for a normal reservation
   rejection, or an error response when the failed step produced an error.
9. If compensation fails, return an error that identifies the rollback failure.

Repeated flight numbers are preserved as separate steps and therefore reserve
multiple seats. A bundle with no flights is rejected even if a car or room was
requested.

### Transaction boundary and caveats

This is a **saga-like compensating sequence**, not an atomic distributed
transaction. During execution, another request can observe intermediate
inventory/customer state. Rollback can fail, and a process crash can interrupt
both forward progress and compensation. The Middleware does not persist a
transaction log or retry a failed compensation after restart.

The compensation method `cancelReservation(customerId, type, identifier)` is an
internal extension on the shared ResourceManager, used by Middleware after a
bundle failure. It restores exactly one unit and decrements the matching
customer reservation count; it removes the reservation entry when its count
reaches zero.

The Middleware stops rollback at the first compensation failure and reports it.
It does not claim the customer's reservation or inventory state is fully
restored in that case.

## Supported API and routing

All methods below are part of `IResourceManager`. "Backend" indicates the
manager that owns the data; "Middleware" indicates additional orchestration.

| API method | Backend / coordinator | Important behavior |
|---|---|---|
| `addFlight(number, seats, price)` | Flights | Creates or extends inventory. For existing inventory, a positive price replaces the price; a nonpositive price preserves it. |
| `addCars(location, count, price)` | Cars | Same price rule as flights. |
| `addRooms(location, count, price)` | Rooms | Same price rule as flights. |
| `newCustomer()` | Middleware + all managers | Registers a generated customer ID on all managers and returns the ID. |
| `newCustomer(id)` | Middleware + all managers | Registers the explicit ID; duplicate IDs report `false`, with cleanup of newly created replicas when possible. |
| `deleteFlight(number)` | Flights | Deletes the item only when it exists and has no reservations. |
| `deleteCars(location)` | Cars | Deletes the item only when it exists and has no reservations. |
| `deleteRooms(location)` | Rooms | Deletes the item only when it exists and has no reservations. |
| `deleteCustomer(id)` | All managers | Restores this manager's reservations and deletes its customer replica; aggregate success requires all managers. |
| `queryFlight(number)` | Flights | Returns available seats; missing items return zero. |
| `queryCars(location)` | Cars | Returns available cars; missing items return zero. |
| `queryRooms(location)` | Rooms | Returns available rooms; missing items return zero. |
| `queryFlightPrice(number)` | Flights | Returns price; missing items return zero. |
| `queryCarsPrice(location)` | Cars | Returns price; missing items return zero. |
| `queryRoomsPrice(location)` | Rooms | Returns price; missing items return zero. |
| `reserveFlight(customerId, number)` | Flights | Returns `false` for a missing customer/item or exhausted inventory. |
| `reserveCar(customerId, location)` | Cars | Returns `false` for a missing customer/item or exhausted inventory. |
| `reserveRoom(customerId, location)` | Rooms | Returns `false` for a missing customer/item or exhausted inventory. |
| `queryCustomerInfo(id)` | All managers | Merges reservation lines and totals; unknown customers yield an empty string. |
| `bundle(id, flights, location, car, room)` | Middleware + managers | Sequential reservation with reverse-order compensation on failure. |
| `getName()` | Middleware | Local health/probe call; returns `"Middleware"`. |

An ordinary business rejection is generally represented by the API's boolean
`false` or numeric zero. Transport, dispatch, and ResourceManager exceptions are
returned as protocol errors and surface at the client as `RemoteException`.
Invalid response types are also surfaced as `RemoteException` rather than
silently coerced.

## Build and run

### Requirements

- A Java runtime and Java compiler compatible with the source.
- `make` for the supplied Makefiles.
- A shell capable of running the scripts under `TCP/Server` and `TCP/Client`.

The integration test script honors `JAVAC` for a compiler executable or
`JAVAC_JAR` for a compiler runnable as `java -jar <path>`. Normally no override
is needed. The override is useful in environments that have a runtime but lack
the `javac` executable.

### Compile

From the repository root:

```sh
make -C TCP
```

This compiles the interface, shared common classes, TCP server classes, and
client classes into `TCP/build`.

### Run the integration tests

```sh
make -C TCP test
```

The test script compiles source and test files into a temporary directory,
executes `Server.TCP.TCPIntegrationTest`, prints the final test line, and
removes the temporary build directory on exit.

### Start the system locally

Run each process in a separate terminal. From the repository root:

```sh
# Terminal 1
TCP/Server/run_server.sh Flights

# Terminal 2
TCP/Server/run_server.sh Cars

# Terminal 3
TCP/Server/run_server.sh Rooms

# Terminal 4
TCP/Server/run_middleware.sh

# Terminal 5
TCP/Client/run_client.sh
```

The server scripts compile the project before launching the requested main
class. Run `make -C TCP clean` when you need to remove the generated
`TCP/build` directory.

### Configure endpoints and ports

The backend server syntax is:

```text
TCPResourceManagerServer <Flights|Cars|Rooms> [port]
```

If omitted, each backend uses its default port (3101, 3102, or 3103).

The Middleware syntax is:

```text
TCPMiddlewareServer [flight_host[:port] [car_host[:port] [room_host[:port] [listen_port]]]]
```

Omitted backend endpoints default to `localhost` and their corresponding
default ports. An endpoint with no explicit port uses that backend's default
port. For example:

```sh
TCP/Server/run_middleware.sh flights.example:3101 cars.example:3102 rooms.example:3103 3042
```

The client syntax is:

```text
TCPClient [middleware_host [middleware_port]]
```

Defaults are `localhost` and `3042`. Each listener must be reachable from the
process connecting to it; backend names are resolved by the operating system.

For programmatic or test startup, port `0` is accepted by the server
constructors and requests an ephemeral port. `start()` returns the actual bound
port. The command-line production entry points use their configured nonzero
ports.

## Integration test guide

The test entry point is
`tests/Server/TCP/TCPIntegrationTest.java`. It starts three backend servers on
ephemeral ports and a Middleware on another ephemeral port. No pre-running
services are needed. It always closes the Middleware and backend servers in a
`finally` block.

The test suite currently verifies the following behaviors:

### Protocol encoding and decoding

- Request IDs and method names survive a round trip.
- Integer, boolean, string, string-vector, and null values survive a round trip.
- Success and error responses survive a round trip.
- An excessive argument count is rejected.
- An unknown response value tag is rejected.

### Request validation

- Unknown Middleware methods return an explicit error.
- A backend method with the wrong argument count returns an error.
- A backend method with the wrong argument type returns an error.
- A Middleware-local method validates its arity.
- A bundle with no flight is rejected.
- A valid request still works after invalid requests.

### Customer, inventory, and reservation behavior

- Explicit customer creation succeeds, while a duplicate ID returns false.
- Generated IDs are positive and registered across managers.
- Adding inventory increments availability and updates prices as expected.
- Query methods return quantities and prices.
- Reservation decreases availability and records a bill line.
- Items with reservations cannot be deleted.
- Customer deletion releases flight, car, and room reservations.
- Unknown customer/item behavior is handled using the established API return
  values.
- Deleting an unreserved item removes it from inventory.
- Repeated reservations aggregate quantity in the bill and restore all units
  when the customer is deleted.
- Nonpositive prices on existing inventory preserve the current price.
- A single unit cannot be reserved twice.

### Bundles

- A complete bundle reserves multiple flights and optional car/room resources.
- The resulting bill contains all bundle reservations.
- An unavailable room causes an earlier successful flight reservation to be
  rolled back.
- Flights-only bundles work when optional resource flags are false.
- Repeated flight numbers consume the requested number of units and aggregate
  in the bill.
- A later unavailable flight causes prior flights in the same bundle to be
  rolled back.
- An unavailable optional car causes prior flight reservations to be rolled
  back.

### Concurrency and failure behavior

- Thirty-two concurrent inventory additions are all applied.
- A deliberately delayed query does not prevent another request to the same
  ResourceManager from completing.
- A separate ResourceManager remains responsive while the flight manager is
  busy.
- Thirty-two concurrent attempts to reserve one unit yield exactly one success.
- Deleting that customer restores the single unit.
- An injected ResourceManager exception reaches the client as
  `RemoteException`.
- The ResourceManager remains usable after a request handler encounters an
  exception.

The concurrency test uses a specialized test-only `SlowResourceManager`. It
delays selected flight queries to expose whether independent worker threads and
request correlation are functioning. That delay is not present in production
`ResourceManager` behavior.

## Errors, timeouts, and shutdown

### Client timeouts and exception mapping

The client uses:

- A 5-second connection timeout when opening a Middleware socket.
- A 30-second socket read timeout while waiting for a response.

Socket and protocol `IOException`s are wrapped in `RemoteException`. An error
response from the Middleware is also surfaced as `RemoteException`. A response
whose ID does not match the request is rejected. Method wrappers additionally
check whether the returned value has the expected Java type.

### Middleware-to-backend connections

The Middleware uses a 5-second connection timeout when it constructs each
backend link. It maintains those sockets for the server lifetime. The current
implementation does not reconnect if a backend disconnects. If the reader
detects a lost backend connection, pending calls that expected that backend
are completed with an error response; future sends to a closed link also fail.
Restart the Middleware after restoring a backend connection.

There is no explicit per-backend request deadline in the pending-call map. The
client's 30-second read timeout bounds how long the calling client waits, but a
pending operation can remain in Middleware state if a backend connection
remains open and never returns a response. A robust production service would
need server-side deadlines and cancellation/cleanup policies.

### Shutdown

The server entry points register JVM shutdown hooks. `close()` stops accepting
new sockets, closes tracked sockets, and shuts down executor pools. Closing
servers is also part of the integration test lifecycle.

Socket close errors during shutdown are intentionally ignored because the
close operation is best-effort cleanup. Operational errors during active
accept and response-write paths are logged to standard error while the process
is running. Backend read failures are converted into errors for pending calls;
malformed client requests are returned as protocol errors when the connection
still permits a response.

## Operational and security considerations

The implementation is suitable as an assignment transport demonstration, not
as a hardened Internet-facing service:

- There is no authentication or authorization. Anyone who can reach the
  Middleware can invoke its API.
- Traffic is plaintext TCP. There is no TLS, peer verification, or encryption
  in transit.
- There is no durable storage. A ResourceManager restart loses its inventory
  and customer state.
- There is no health-check endpoint beyond the `getName()` API call.
- Middleware uses one backend connection per manager and has no reconnect
  strategy.
- Request workers use bounded fixed thread pools, but accept/connection
  handlers use cached pools and there are no explicit connection quotas.
- Wire value and collection counts are bounded, but `writeUTF` strings and
  overall resource consumption are not governed by a comprehensive
  application-level message-size policy.
- A client timeout does not necessarily cancel work already accepted by the
  Middleware.
- Customer replication and bundle compensation can be left partially applied
  by crashes, network failures, or compensation failures.
- The generated customer ID counter is process-local and not durable.

Do not expose the ports to an untrusted network without adding authentication,
authorization, TLS, connection/resource limits, durable state, timeouts,
reconnection behavior, and a recovery strategy appropriate to the deployment.

## Extension guidance

### Adding an API method

For a new `IResourceManager` operation:

1. Add or confirm its signature in `Server/Interface/IResourceManager.java`.
2. Implement the domain behavior in the appropriate shared
   `ResourceManager` class or in Middleware if it spans managers.
3. For backend-owned methods, add an explicit method-name case to
   `ResourceManagerDispatcher.invoke`.
4. Add the matching method to `TcpResourceManagerClient`, using `invoke` and
   a typed response helper.
5. Add a routing entry in `TCPMiddlewareServer.resourceFor` for a new
   backend-owned method. Middleware-local or multi-backend methods should be
   handled explicitly in `route`.
6. Confirm that all arguments and return values are supported by
   `TcpProtocol`. Extend the protocol deliberately if a new value type is
   required; retain decoding bounds and malformed-input checks.
7. Add positive, negative, malformed-argument, and concurrency tests as
   appropriate.
8. Run `make -C TCP test` and `git diff --check`.

Keep the method mapping explicit and typed. Do not use arbitrary reflection to
dispatch untrusted method names.

### Adding a resource category

A fourth inventory category would require coordinated changes to:

- `ResourceType` names and default ports.
- Backend launch configuration and endpoint mapping.
- Middleware routing and any all-manager operations.
- Shared model classes and `ResourceManager` operations.
- Customer replication and bill aggregation assumptions.
- Tests and this documentation.

Review code that relies on `ResourceType.values()` or expects exactly three
managers. Some operations deliberately use all resource types, while others
are routed to exactly one manager.

### Preserving behavior

Use the existing `IResourceManager` contract and shared RMI behavior as the
compatibility baseline. Keep application semantics separate from transport
details: TCP framing and dispatch belong in the TCP layer, while inventory
rules belong in the shared domain model unless the behavior inherently spans
ResourceManagers.

## Source map

| File | Responsibility |
|---|---|
| `Server/Server/TCP/TcpProtocol.java` | Typed request/response serialization and decoding bounds. |
| `Server/Server/TCP/ResourceManagerDispatcher.java` | Explicit backend method allow-list, argument validation, and invocation. |
| `Server/Server/TCP/ResourceType.java` | Resource category names and default backend ports. |
| `Server/Server/TCP/TCPResourceManagerServer.java` | Backend accept loop, connection handling, worker dispatch, response writing, and process entry point. |
| `Server/Server/TCP/TCPMiddlewareServer.java` | Client routing, backend links, pending-call correlation, customer coordination, bill aggregation, bundle execution, and process entry point. |
| `Client/Client/TcpResourceManagerClient.java` | `IResourceManager` adapter for the TCP protocol. |
| `Client/Client/TCPClient.java` | Interactive TCP client entry point and connection probe. |
| `Server/Server/Common/ResourceManager.java` | Shared in-memory inventory, reservations, customer operations, and compensation primitive. |
| `Server/Server/Common/Customer.java` | Customer reservation records and bill formatting. |
| `Server/Server/Interface/IResourceManager.java` | Public API contract shared with the RMI implementation. |
| `tests/Server/TCP/TCPIntegrationTest.java` | End-to-end protocol, API, bundle, concurrency, and failure-path tests. |
| `test.sh` | Temporary-directory compilation and test execution. |
| `Makefile` | Main compile, test, and clean targets. |
