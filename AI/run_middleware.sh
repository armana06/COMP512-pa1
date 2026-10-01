#!/bin/bash
# Usage: ./run_middleware.sh <flights_host> <cars_host> <rooms_host>
#   $1 - hostname of Flights
#   $2 - hostname of Cars
#   $3 - hostname of Rooms
# (For a purely local test: ./run_middleware.sh localhost localhost localhost)
 
# Start an rmiregistry on this machine (port 1099) in the background.
# Output is discarded: if one is already running, this just fails harmlessly
# with "port already in use". Same line as run_server.sh.
./run_rmi.sh > /dev/null 2>&1
 
# Launch the middleware. The codebase flag tells the registry where to find the
# IResourceManager class (same flag as run_server.sh).
java -Djava.rmi.server.codebase=file:$(pwd)/ Server.RMI.RMIMiddleware $1 $2 $3
