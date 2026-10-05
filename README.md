# comp512 programming assignment 1

To run the RMI resource manager:

```
cd Server/
./run_server.sh [<rmi_name>] # starts a single ResourceManager
./run_servers.sh # convenience script for starting multiple resource managers
```

To run the RMI client:

```
cd Client
./run_client.sh [<server_hostname> [<server_rmi_name>]]
```

## TCP implementation

The TCP implementation is in `TCP/` and is independent of the RMI runtime. It
uses the same `IResourceManager` API and shared reservation data model as the
RMI implementation.

Build and run the integration suite:

```
cd TCP
make
make test
```

For a local system, start one process per terminal:

```
cd TCP/Server
# Terminal 1
./run_server.sh Flights
# Terminal 2
./run_server.sh Cars
# Terminal 3
./run_server.sh Rooms
# Terminal 4
./run_middleware.sh
```

Then start the client:

```
cd TCP/Client
./run_client.sh [<middleware_host> [<middleware_port>]]
```

The ResourceManagers listen on ports 3101 (Flights), 3102 (Cars), and 3103
(Rooms); the Middleware listens on port 3042. The Middleware arguments can
override backend endpoints as `host[:port]` values, in Flights/Cars/Rooms
order, followed by an optional Middleware port. For example:

```
./run_middleware.sh flights.example:3101 cars.example:3102 rooms.example:3103 3042
```

The middleware keeps one connection to each ResourceManager. Requests carry
correlation IDs over a typed TCP protocol; the middleware keeps accepting
clients while backend work runs, and routes asynchronous backend replies to
the waiting client. ResourceManagers use worker pools for concurrent requests.
Customer operations are replicated to all three managers, and bundle
reservations compensate completed steps if a later reservation fails.

For implementation details, API behavior, protocol layout, testing coverage,
limitations, and extension guidance, see [TCP/README.md](TCP/README.md).
