# RMI middleware (AI version)

Conversation: https://claude.ai/share/ed28011a-42ed-44b7-b5f0-b539f9a87bba

## Drop-in replacement

Copy these two files into the template, then run `make` in `Server` and `Client`:

- `Server/Server/RMI/RMIMiddleware.java`
- `Server/run_middleware.sh`

Nothing else in the template needs to change, except the port below.

## Port

The middleware uses port 3042, so change `1099` to `3042` in:

- `Client/Client/RMIClient.java` (line 17)
- `Server/Server/RMI/RMIResourceManager.java` (lines 39 and 41)
- `Server/run_rmi.sh` (line 2)

## Running it

I only tested locally.

```
# in Server/
./run_server.sh Flights
./run_server.sh Cars
./run_server.sh Rooms
./run_middleware.sh localhost localhost localhost

# in Client/
./run_client.sh localhost Middleware
```

## Disclaimer

I used Claude to generate this markdown file because, well, why would I write a markdown file myself?