// -------------------------------
// COMP 512 - Programming Assignment 1
// RMI Middleware
// -------------------------------
//
// What this class is:
//   A server that sits between the client and the three ResourceManagers (RMs).
//   It implements IResourceManager, which is exactly the interface the client
//   already uses, so the client can talk to the Middleware without any change.
//
// Status by step:
//   [step 1] connect to the RMs (retry loop), export + bind as "group_42_Middleware",
//            getName() returns "Middleware"
//   [step 2] the 15 flight / car / room methods are forwarded to the matching RM
//   [step 3] customers are replicated on all 3 RMs (create / delete / merged bill)
//   [step 4] bundle: validate + pre-check everything, then reserve, all-or-nothing
//            with respect to bad input and concurrent clients
//
// Concurrency (decision): ONE global lock (m_lock) serializes everything that
// modifies state (bundle, add*, reserve*, delete*, create / delete customer) plus
// queryCustomerInfo, whose bill is assembled from three RMs and must not be read
// while a bundle or customer operation is half done. The single-RM item queries
// (queryFlight / queryCars / queryRooms and the three price queries) are not locked.
//
// Error handling (decision): a RemoteException coming from an RM (e.g. the RM is
// down) is deliberately NOT caught here. It propagates to the client, which
// already prints it as a "Command exception".
//
// Timeout: every call the middleware makes to an RM gives up after s_rmTimeoutMs
// (see main), so a HUNG RM becomes a RemoteException instead of blocking the global
// lock forever. A timeout means "outcome unknown": the RM may still run the call later.
//
// Input check: add* rejects a negative count (returns false); zero is allowed.

package Server.RMI;

import Server.Common.Trace;
import Server.Interface.IResourceManager;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class RMIMiddleware implements IResourceManager
{
	// ---------------------------------------------------------------
	// Constants (hardcoded for now, as agreed)
	// ---------------------------------------------------------------

	// Registry port. This ONE constant is used both to reach the RMs' registries and
	// for the middleware's own registry, so RMIResourceManager, RMIClient and
	// run_rmi.sh must all use the same value (3042). The prefix is prepended to
	// every name we bind or look up in the RMI registry.
	private static final int    s_rmiPort   = 3042;
	private static final String s_rmiPrefix = "group_42_";

	// The name the client asks for:  ./run_client.sh <host> Middleware
	private static final String s_serverName = "Middleware";

	// The names the RMs are started with (see run_servers.sh:
	// ./run_server.sh Flights | Cars | Rooms).
	private static final String s_flightsName = "Flights";
	private static final String s_carsName    = "Cars";
	private static final String s_roomsName   = "Rooms";

	// How long (ms) the middleware waits for an RM before giving up on a call.
	// Without it, a hung (not dead) RM would block the global lock forever.
	// main() applies it to two RMI properties, because they cover different cases:
	//   responseTimeout  : waiting for the answer on an established connection
	//   handshakeTimeout : opening a NEW connection (the pool closes idle ones)
	// Measured with a frozen RM: a call fails after up to about twice this value
	// (~10 s), and calls to the other RMs are unaffected. Not covered: an unreachable
	// host whose TCP connect hangs; that is limited only by the OS connect timeout.
	private static final int s_rmTimeoutMs = 5000;

	// ---------------------------------------------------------------
	// State: one RMI stub per RM
	// ---------------------------------------------------------------

	// A stub is a local proxy object: calling a method on it sends the call over
	// the network to the real RM. They are final and set in the constructor, so
	// an RMIMiddleware object never exists without all three connections.
	// Step 2: every flight / car / room method below forwards to one of these.
	private final IResourceManager m_flightsRM;
	private final IResourceManager m_carsRM;
	private final IResourceManager m_roomsRM;

	// The same three stubs as an array, in a FIXED order (Flights, Cars, Rooms), plus
	// their names. Customer operations (step 3) must visit all RMs, so they loop
	// over these. The fixed order also matters for the undo logic.
	private final IResourceManager[] m_allRMs;
	private static final String[] s_allRMNames = { s_flightsName, s_carsName, s_roomsName };

	// The ONE global lock. It is held by:
	//   - addFlight / addCars / addRooms: the RM's add is a read-modify-write, so an
	//     unlocked add could overlap with a reserve and LOSE an update (stock drifts),
	//     or lower stock under a bundle's pre-check
	//   - reserveFlight / reserveCar / reserveRoom and deleteFlight / deleteCars /
	//     deleteRooms (each can invalidate a bundle's availability pre-check)
	//   - newCustomer / deleteCustomer (touch 3 RMs in sequence)
	//   - queryCustomerInfo (three reads that must see a consistent state)
	//   - bundle (pre-check + reserves must look like one atomic step)
	// Since every client goes through the middleware, this is enough to make the
	// pre-check stay true until the reserves are done. It also guards m_nextCustomerID.
	// Trade-off: these operations are fully serialized (accepted for this assignment).
	// The item queries (queryFlight / queryCars / queryRooms, price queries) are each
	// a single atomic read on one RM, so they stay unlocked.
	private final Object m_lock = new Object();

	// Next ID that AddCustomer will try. In-memory only, starts at 1. If it is
	// taken (AddCustomerID used it, or we restarted), we just move to the next one.
	private int m_nextCustomerID = 1;

	public RMIMiddleware(IResourceManager flightsRM, IResourceManager carsRM, IResourceManager roomsRM)
	{
		m_flightsRM = flightsRM;
		m_carsRM = carsRM;
		m_roomsRM = roomsRM;
		m_allRMs = new IResourceManager[] { flightsRM, carsRM, roomsRM };
	}

	// ---------------------------------------------------------------
	// Startup
	// ---------------------------------------------------------------

	// Usage: RMIMiddleware <flights_host> <cars_host> <rooms_host>
	// This matches the $1 $2 $3 convention of run_middleware.sh / run_servers.sh.
	public static void main(String args[])
	{
		if (args.length != 3)
		{
			System.err.println((char)27 + "[31;1mMiddleware exception: " + (char)27 + "[0mUsage: java Server.RMI.RMIMiddleware <flights_host> <cars_host> <rooms_host>");
			System.exit(1);
		}

		// Timeouts for every call this JVM makes to an RM (see s_rmTimeoutMs). The RMI
		// runtime reads these properties once, when it first opens a connection, so
		// they must be set before any RMI call below.
		System.setProperty("sun.rmi.transport.tcp.responseTimeout", String.valueOf(s_rmTimeoutMs));
		System.setProperty("sun.rmi.transport.tcp.handshakeTimeout", String.valueOf(s_rmTimeoutMs));

		try {
			// 1. Connect to the three RMs. Each call blocks (polling every 500 ms)
			//    until that RM is reachable, so start-up order does not matter.
			IResourceManager flightsRM = connectToRM(args[0], s_flightsName);
			IResourceManager carsRM    = connectToRM(args[1], s_carsName);
			IResourceManager roomsRM   = connectToRM(args[2], s_roomsName);

			// 2. Build the middleware. We only do this AFTER all three RMs are up,
			//    and bind in the registry AFTER that, so a client can never find
			//    a middleware that is not ready to forward requests.
			RMIMiddleware middleware = new RMIMiddleware(flightsRM, carsRM, roomsRM);

			// 3. Export the object: this makes it able to receive remote calls and
			//    returns its stub (the proxy the client will download from the
			//    registry). Port 0 means "pick any free port" for that endpoint.
			IResourceManager stub = (IResourceManager)UnicastRemoteObject.exportObject(middleware, 0);

			// 4. Publish the stub in the registry under "group_42_Middleware".
			//    rebind (not bind) overwrites a stale entry from a previous run.
			final Registry registry = getOrCreateRegistry();
			registry.rebind(s_rmiPrefix + s_serverName, stub);

			// 5. On shutdown (Ctrl-C), remove our entry so the registry is not left
			//    pointing at a dead object. Same idea as RMIResourceManager.
			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					try {
						registry.unbind(s_rmiPrefix + s_serverName);
						System.out.println("'" + s_serverName + "' middleware unbound");
					}
					catch (Exception e) {
						System.err.println((char)27 + "[31;1mMiddleware exception: " + (char)27 + "[0mUncaught exception");
						e.printStackTrace();
					}
				}
			});

			System.out.println("'" + s_serverName + "' middleware ready and bound to '" + s_rmiPrefix + s_serverName + "'");
			// main() ends here but the JVM stays alive: the exported object keeps
			// RMI's non-daemon threads running, exactly like the RMs.
		}
		catch (Exception e) {
			System.err.println((char)27 + "[31;1mMiddleware exception: " + (char)27 + "[0mUncaught exception");
			e.printStackTrace();
			System.exit(1);
		}
	}

	// Looks up the RM called <name> in the registry running on <host>, retrying
	// every 500 ms until it succeeds (same pattern as RMIClient.connectServer).
	private static IResourceManager connectToRM(String host, String name) throws InterruptedException
	{
		String boundName = s_rmiPrefix + name;
		boolean first = true;
		while (true)
		{
			try {
				Registry registry = LocateRegistry.getRegistry(host, s_rmiPort);
				IResourceManager rm = (IResourceManager)registry.lookup(boundName);

				// Sanity check: actually call the RM once. A successful lookup only
				// proves the name is in the registry; it could be a stale entry left
				// by a crashed RM. If the RM is dead, this throws a RemoteException
				// and we simply keep waiting.
				String reportedName = rm.getName();

				System.out.println("Connected to '" + name + "' resource manager [" + host + ":" + s_rmiPort + "/" + boundName + "] (reports name '" + reportedName + "')");
				return rm;
			}
			catch (NotBoundException|RemoteException e) {
				// NotBoundException: RM not registered yet.
				// RemoteException:   no registry / RM unreachable (e.g. ConnectException).
				// Print the "waiting" line once, not every 500 ms.
				if (first) {
					System.out.println("Waiting for '" + name + "' resource manager [" + host + ":" + s_rmiPort + "/" + boundName + "]");
					first = false;
				}
			}
			Thread.sleep(500);
		}
	}

	// The middleware binds itself in the registry of ITS OWN machine, so it needs
	// one there. Try to create it; if one already exists on this port (e.g. the
	// rmiregistry started by run_rmi.sh), use that one. Same trick as
	// RMIResourceManager.
	private static Registry getOrCreateRegistry() throws RemoteException
	{
		try {
			return LocateRegistry.createRegistry(s_rmiPort);
		}
		catch (RemoteException e) {
			return LocateRegistry.getRegistry(s_rmiPort);
		}
	}

	// ---------------------------------------------------------------
	// IResourceManager -- implemented
	// ---------------------------------------------------------------

	public String getName() throws RemoteException
	{
		return "Middleware";
	}

	// ---------------------------------------------------------------
	// IResourceManager -- flights (step 2: forwarded to the Flights RM)
	// ---------------------------------------------------------------

	public boolean addFlight(int flightNum, int flightSeats, int flightPrice) throws RemoteException
	{
		// A negative count is rejected here: it would reduce stock (and, with the
		// template's reserveItem, even allow negative stock). Zero is allowed: adding
		// 0 seats is how an existing item's price is updated.
		if (flightSeats < 0)
		{
			Trace.warn("RMIMiddleware::addFlight rejected -- negative count (" + flightSeats + ")");
			return false;
		}
		// Takes the global lock (see m_lock): the RM's add is a read-modify-write that
		// must not overlap with a reserve or a bundle.
		synchronized (m_lock)
		{
			Trace.info("RMIMiddleware::addFlight -> Flights RM");
			return m_flightsRM.addFlight(flightNum, flightSeats, flightPrice);
		}
	}

	public boolean deleteFlight(int flightNum) throws RemoteException
	{
		// Takes the global lock: deleting an item must not happen between a bundle's
		// pre-check and its reserves.
		synchronized (m_lock)
		{
			Trace.info("RMIMiddleware::deleteFlight -> Flights RM");
			return m_flightsRM.deleteFlight(flightNum);
		}
	}

	public int queryFlight(int flightNumber) throws RemoteException
	{
		Trace.info("RMIMiddleware::queryFlight -> Flights RM");
		return m_flightsRM.queryFlight(flightNumber);
	}

	public int queryFlightPrice(int flightNumber) throws RemoteException
	{
		Trace.info("RMIMiddleware::queryFlightPrice -> Flights RM");
		return m_flightsRM.queryFlightPrice(flightNumber);
	}

	public boolean reserveFlight(int customerID, int flightNumber) throws RemoteException
	{
		// Customers are replicated on every RM (step 3), so the customer exists on this
		// RM and ResourceManager.reserveItem() works unchanged: a plain forward.
		// It takes the global lock so it cannot slip in between a bundle's availability
		// pre-check and its reserves (step 4).
		synchronized (m_lock)
		{
			Trace.info("RMIMiddleware::reserveFlight -> Flights RM");
			return m_flightsRM.reserveFlight(customerID, flightNumber);
		}
	}

	// ---------------------------------------------------------------
	// IResourceManager -- cars (step 2: forwarded to the Cars RM)
	// ---------------------------------------------------------------

	public boolean addCars(String location, int numCars, int price) throws RemoteException
	{
		// A negative count is rejected here: it would reduce stock (and, with the
		// template's reserveItem, even allow negative stock). Zero is allowed: adding
		// 0 cars is how an existing item's price is updated.
		if (numCars < 0)
		{
			Trace.warn("RMIMiddleware::addCars rejected -- negative count (" + numCars + ")");
			return false;
		}
		// Takes the global lock (see m_lock): the RM's add is a read-modify-write that
		// must not overlap with a reserve or a bundle.
		synchronized (m_lock)
		{
			Trace.info("RMIMiddleware::addCars -> Cars RM");
			return m_carsRM.addCars(location, numCars, price);
		}
	}

	public boolean deleteCars(String location) throws RemoteException
	{
		// Takes the global lock: deleting an item must not happen between a bundle's
		// pre-check and its reserves.
		synchronized (m_lock)
		{
			Trace.info("RMIMiddleware::deleteCars -> Cars RM");
			return m_carsRM.deleteCars(location);
		}
	}

	public int queryCars(String location) throws RemoteException
	{
		Trace.info("RMIMiddleware::queryCars -> Cars RM");
		return m_carsRM.queryCars(location);
	}

	public int queryCarsPrice(String location) throws RemoteException
	{
		Trace.info("RMIMiddleware::queryCarsPrice -> Cars RM");
		return m_carsRM.queryCarsPrice(location);
	}

	public boolean reserveCar(int customerID, String location) throws RemoteException
	{
		// Customers are replicated on every RM (step 3), so the customer exists on this
		// RM and ResourceManager.reserveItem() works unchanged: a plain forward.
		// It takes the global lock so it cannot slip in between a bundle's availability
		// pre-check and its reserves (step 4).
		synchronized (m_lock)
		{
			Trace.info("RMIMiddleware::reserveCar -> Cars RM");
			return m_carsRM.reserveCar(customerID, location);
		}
	}

	// ---------------------------------------------------------------
	// IResourceManager -- rooms (step 2: forwarded to the Rooms RM)
	// ---------------------------------------------------------------

	public boolean addRooms(String location, int numRooms, int price) throws RemoteException
	{
		// A negative count is rejected here: it would reduce stock (and, with the
		// template's reserveItem, even allow negative stock). Zero is allowed: adding
		// 0 rooms is how an existing item's price is updated.
		if (numRooms < 0)
		{
			Trace.warn("RMIMiddleware::addRooms rejected -- negative count (" + numRooms + ")");
			return false;
		}
		// Takes the global lock (see m_lock): the RM's add is a read-modify-write that
		// must not overlap with a reserve or a bundle.
		synchronized (m_lock)
		{
			Trace.info("RMIMiddleware::addRooms -> Rooms RM");
			return m_roomsRM.addRooms(location, numRooms, price);
		}
	}

	public boolean deleteRooms(String location) throws RemoteException
	{
		// Takes the global lock: deleting an item must not happen between a bundle's
		// pre-check and its reserves.
		synchronized (m_lock)
		{
			Trace.info("RMIMiddleware::deleteRooms -> Rooms RM");
			return m_roomsRM.deleteRooms(location);
		}
	}

	public int queryRooms(String location) throws RemoteException
	{
		Trace.info("RMIMiddleware::queryRooms -> Rooms RM");
		return m_roomsRM.queryRooms(location);
	}

	public int queryRoomsPrice(String location) throws RemoteException
	{
		Trace.info("RMIMiddleware::queryRoomsPrice -> Rooms RM");
		return m_roomsRM.queryRoomsPrice(location);
	}

	public boolean reserveRoom(int customerID, String location) throws RemoteException
	{
		// Customers are replicated on every RM (step 3), so the customer exists on this
		// RM and ResourceManager.reserveItem() works unchanged: a plain forward.
		// It takes the global lock so it cannot slip in between a bundle's availability
		// pre-check and its reserves (step 4).
		synchronized (m_lock)
		{
			Trace.info("RMIMiddleware::reserveRoom -> Rooms RM");
			return m_roomsRM.reserveRoom(customerID, location);
		}
	}

	// ---------------------------------------------------------------
	// IResourceManager -- customers (step 3: replicated on all 3 RMs)
	// ---------------------------------------------------------------
	//
	// Design: every customer exists on all three RMs. Each RM's copy only holds
	// the reservations for ITS resource type. This is what keeps reserveFlight /
	// reserveCar / reserveRoom a plain forward: the customer and the item are in
	// the same RM's map, as ResourceManager.reserveItem requires.
	//
	// Failure policy (best effort, then propagate):
	//   create : if an RM fails, undo the creations that succeeded, then rethrow
	//   delete : try every RM even if one fails, then report which ones failed
	//   query  : any unreachable RM fails the whole query (a partial bill would
	//            have a wrong total)

	// AddCustomer: hand out sequential IDs, skipping any that are already taken.
	public int newCustomer() throws RemoteException
	{
		synchronized (m_lock)
		{
			while (true)
			{
				int cid = m_nextCustomerID++;
				if (createCustomerEverywhere(cid))
				{
					Trace.info("RMIMiddleware::newCustomer() created customer " + cid + " on all RMs");
					return cid;
				}
				// false means "ID already taken": loop and try the next one.
				// A RemoteException is NOT caught, so a broken RM ends the loop
				// instead of making it spin forever.
			}
		}
	}

	// AddCustomerID: true only if the ID was free on all three RMs.
	public boolean newCustomer(int cid) throws RemoteException
	{
		synchronized (m_lock)
		{
			boolean ok = createCustomerEverywhere(cid);
			Trace.info("RMIMiddleware::newCustomer(" + cid + ") -> " + ok);
			return ok;
		}
	}

	// Creates customer <cid> on every RM. Caller must hold m_lock.
	//   true  : created on all 3 RMs
	//   false : ID is taken. Either taken on all 3, or taken on only some (a
	//           "mixed" state, e.g. an orphan from an earlier failure). In the mixed
	//           case we undo what we just created, so we leave nothing behind.
	//   throws: an RM was unreachable. Whatever we created before it is undone first.
	private boolean createCustomerEverywhere(int cid) throws RemoteException
	{
		boolean[] created = new boolean[m_allRMs.length];
		int createdCount = 0;
		try {
			for (int i = 0; i < m_allRMs.length; i++)
			{
				// If this call throws, created[i] stays false. We can't know whether
				// the RM executed it before failing, so we don't try to undo it.
				created[i] = m_allRMs[i].newCustomer(cid);
				if (created[i]) createdCount++;
			}
		}
		catch (RemoteException e) {
			undoCustomerCreation(cid, created);
			throw e;
		}

		if (createdCount == m_allRMs.length)
		{
			return true;
		}
		if (createdCount > 0)
		{
			Trace.warn("RMIMiddleware: customer " + cid + " already existed on some RMs only; undoing partial creation");
			undoCustomerCreation(cid, created);
		}
		return false;
	}

	// Best-effort undo: deletes customer <cid> on the RMs where WE just created it.
	// This is safe because a brand-new customer has no reservations to release.
	// We never touch RMs where newCustomer returned false (that customer was there
	// before us). If the undo itself fails we only log it: the exception the caller
	// is about to see is more important, and there is nothing else we can do.
	private void undoCustomerCreation(int cid, boolean[] created)
	{
		for (int i = 0; i < m_allRMs.length; i++)
		{
			if (!created[i]) continue;
			try {
				m_allRMs[i].deleteCustomer(cid);
			}
			catch (RemoteException e) {
				Trace.error("RMIMiddleware: could not undo creation of customer " + cid + " on " + s_allRMNames[i] + " -> orphan left behind");
			}
		}
	}

	// Deletes the customer on every RM; each RM releases its own reservations.
	// Unlike create, this can't be undone (the interface cannot restore
	// reservations), so instead of stopping at the first failure we keep going:
	// that leaves as few RMs as possible still holding the customer.
	//   true  : at least one RM actually deleted the customer
	//   false : no RM knew the customer
	//   throws: one or more RMs were unreachable; the message names them.
	//           The operation is safe to retry: RMs that already deleted the
	//           customer just return false the second time.
	public boolean deleteCustomer(int customerID) throws RemoteException
	{
		synchronized (m_lock)
		{
			boolean anyDeleted = false;
			List<String> failedRMs = new ArrayList<String>();
			RemoteException firstFailure = null;

			for (int i = 0; i < m_allRMs.length; i++)
			{
				try {
					if (m_allRMs[i].deleteCustomer(customerID)) anyDeleted = true;
				}
				catch (RemoteException e) {
					failedRMs.add(s_allRMNames[i]);
					if (firstFailure == null) firstFailure = e;
				}
			}

			if (!failedRMs.isEmpty())
			{
				throw new RemoteException("Middleware: deleteCustomer(" + customerID + ") incomplete, could not reach " + failedRMs + ". The other RMs were processed; retry the command to finish.", firstFailure);
			}
			Trace.info("RMIMiddleware::deleteCustomer(" + customerID + ") -> " + anyDeleted);
			return anyDeleted;
		}
	}

	// Asks every RM for its part of the bill and merges them (see mergeBills).
	// Takes the global lock: the bill is assembled from three separate reads, and
	// without the lock a bundle or a customer create / delete running at the same
	// time could be seen half done (e.g. the flights already on the bill but not
	// the car yet). An unreachable RM makes the exception propagate.
	public String queryCustomerInfo(int customerID) throws RemoteException
	{
		synchronized (m_lock)
		{
			String[] bills = new String[m_allRMs.length];
			for (int i = 0; i < m_allRMs.length; i++)
			{
				bills[i] = m_allRMs[i].queryCustomerInfo(customerID);
			}
			Trace.info("RMIMiddleware::queryCustomerInfo(" + customerID + ") merging bills from all RMs");
			return mergeBills(customerID, bills);
		}
	}

	// Merges the per-RM bills into one. What an RM returns (Customer.getBill):
	//     Bill for customer <id>\n
	//     <count> <itemKey> $<price>\n      (one line per reserved item type)
	// or "" if that RM does not know the customer.
	// Result: one header, all item lines (Flights, then Cars, then Rooms), then
	//     Total: $<sum of count * price>
	// The total uses the prices printed on the lines, i.e. the price stored with
	// the reservation (the latest price at which the customer reserved that
	// item), NOT the item's current catalog price.
	// Returns "" if no RM knows the customer (same as the template).
	//
	// NOTE: this parses text produced by Customer.getBill(). If that format
	// changes, this must change with it.
	private static String mergeBills(int cid, String[] bills) throws RemoteException
	{
		boolean known = false;
		StringBuilder itemLines = new StringBuilder();
		long total = 0;   // long so count * price can't overflow an int

		for (String bill : bills)
		{
			if (bill == null || bill.isEmpty()) continue;   // this RM doesn't know the customer
			known = true;

			String[] lines = bill.split("\n");
			for (int i = 1; i < lines.length; i++)           // i = 1 skips the header line
			{
				String line = lines[i];
				if (line.trim().isEmpty()) continue;
				try {
					// Count is everything before the first space; price is everything
					// after the last " $". The key in between may contain spaces
					// (e.g. location "new york"), so we can't split on spaces.
					int count = Integer.parseInt(line.substring(0, line.indexOf(' ')));
					int price = Integer.parseInt(line.substring(line.lastIndexOf(" $") + 2));
					total += (long)count * price;
				}
				catch (NumberFormatException|StringIndexOutOfBoundsException e) {
					throw new RemoteException("Middleware: cannot parse bill line '" + line + "'", e);
				}
				itemLines.append(line).append("\n");
			}
		}

		if (!known) return "";
		return "Bill for customer " + cid + "\n" + itemLines + "Total: $" + total + "\n";
	}

	// ---------------------------------------------------------------
	// IResourceManager -- bundle (step 4)
	// ---------------------------------------------------------------
	//
	// Reserves N flights (N may be 0) and, optionally, a car and/or a room at <location>
	// for one customer, ALL-OR-NOTHING. Strategy: check everything first, and only
	// if every check passes start reserving. The whole method holds the global lock,
	// so no other reserve / delete / bundle can change the state between the checks
	// and the reserves.
	//
	//   returns true  : everything was reserved
	//   returns false : a check failed; NOTHING was reserved
	//   throws        : an RM was unreachable
	//                   - during the checks: nothing was reserved
	//                   - during the reserves: the message lists what WAS reserved
	//                     (see reserveBundleItems; this is the one non-atomic window)
	//
	// Order of reservations: flights as given, then the car, then the room.
	// A flight number repeated in the list means one seat per occurrence.
	// If neither car nor room is requested, <location> is ignored.
	public boolean bundle(int customerID, Vector<String> flightNumbers, String location, boolean car, boolean room) throws RemoteException
	{
		synchronized (m_lock)
		{
			Trace.info("RMIMiddleware::bundle(customer=" + customerID + ", flights=" + flightNumbers + ", location=" + location + ", car=" + car + ", room=" + room + ") called");

			// ---- Phase 0: validate the request locally (no RM is contacted) ----

			// No minimum number of flights is enforced here. A null list is treated
			// like an empty one (zero flights), so such a bundle only reserves the
			// car and/or room, or does nothing at all and trivially succeeds.
			// (The provided client never sends zero flights; see Client.java.)

			// Parse the flight numbers (they arrive as strings) and count how many
			// seats each distinct flight needs. LinkedHashMap keeps the first-seen order.
			int[] flights = new int[flightNumbers == null ? 0 : flightNumbers.size()];
			Map<Integer, Integer> seatsNeeded = new LinkedHashMap<Integer, Integer>();
			for (int i = 0; i < flights.length; i++)
			{
				String text = flightNumbers.get(i);
				try {
					flights[i] = Integer.parseInt(text == null ? "" : text.trim());
				}
				catch (NumberFormatException e) {
					Trace.warn("RMIMiddleware::bundle failed -- '" + text + "' is not a flight number");
					return false;
				}
				Integer seats = seatsNeeded.get(flights[i]);
				seatsNeeded.put(flights[i], seats == null ? 1 : seats + 1);
			}

			// ---- Phase 1: pre-check (read-only; nothing is modified) ----
			// Any RemoteException here simply propagates: nothing has changed yet.

			// The customer must exist on every RM this bundle will touch. (An unknown
			// customer is reported as an empty bill; a known one always has a header.)
			if (m_flightsRM.queryCustomerInfo(customerID).isEmpty()
				|| (car && m_carsRM.queryCustomerInfo(customerID).isEmpty())
				|| (room && m_roomsRM.queryCustomerInfo(customerID).isEmpty()))
			{
				Trace.warn("RMIMiddleware::bundle failed -- customer " + customerID + " does not exist");
				return false;
			}

			// Each distinct flight needs enough seats for ALL its occurrences. A
			// missing flight reports 0 seats, so it fails this check too.
			for (Map.Entry<Integer, Integer> entry : seatsNeeded.entrySet())
			{
				int available = m_flightsRM.queryFlight(entry.getKey());
				if (available < entry.getValue())
				{
					Trace.warn("RMIMiddleware::bundle failed -- flight " + entry.getKey() + " has " + available + " seat(s), " + entry.getValue() + " needed");
					return false;
				}
			}

			if (car && m_carsRM.queryCars(location) < 1)
			{
				Trace.warn("RMIMiddleware::bundle failed -- no car available at " + location);
				return false;
			}
			if (room && m_roomsRM.queryRooms(location) < 1)
			{
				Trace.warn("RMIMiddleware::bundle failed -- no room available at " + location);
				return false;
			}

			// ---- Phase 2: reserve ----
			boolean ok = reserveBundleItems(customerID, flights, location, car, room);
			Trace.info("RMIMiddleware::bundle(customer=" + customerID + ") -> " + ok);
			return ok;
		}
	}

	// Phase 2 of bundle: performs the reservations one by one, in order. After a
	// successful pre-check under the lock every call should succeed, so failing here
	// means something unusual happened: an RM died, or someone changed an RM
	// directly, bypassing the middleware. There is no way to cancel a single
	// reservation, so we cannot roll back. Instead we never hide the damage:
	//   - failure on the very first reservation -> nothing changed: rethrow / return false
	//   - failure after some reservations       -> throw an exception that lists
	//     exactly which reservations were made and stay in place
	// Caller must hold m_lock.
	private boolean reserveBundleItems(int customerID, int[] flights, String location, boolean car, boolean room) throws RemoteException
	{
		List<String> done = new ArrayList<String>();   // what has been reserved so far
		String current = "";                           // what we are trying right now
		boolean refused = false;                       // an RM answered false

		try {
			for (int i = 0; i < flights.length && !refused; i++)
			{
				current = "flight " + flights[i];
				if (m_flightsRM.reserveFlight(customerID, flights[i])) done.add(current);
				else refused = true;
			}
			if (car && !refused)
			{
				current = "car at " + location;
				if (m_carsRM.reserveCar(customerID, location)) done.add(current);
				else refused = true;
			}
			if (room && !refused)
			{
				current = "room at " + location;
				if (m_roomsRM.reserveRoom(customerID, location)) done.add(current);
				else refused = true;
			}
		}
		catch (RemoteException e) {
			if (done.isEmpty()) throw e;   // nothing had been reserved yet
			throw new RemoteException("Middleware: bundle for customer " + customerID + " PARTIALLY applied. Reserved and NOT rolled back: " + done + ". Then failed on: " + current + " (outcome unknown: it may or may not have been applied).", e);
		}

		if (refused)
		{
			if (done.isEmpty())
			{
				Trace.warn("RMIMiddleware::bundle failed -- RM refused " + current);
				return false;
			}
			throw new RemoteException("Middleware: bundle for customer " + customerID + " PARTIALLY applied. Reserved and NOT rolled back: " + done + ". Then the RM refused: " + current + " (was an RM modified directly, bypassing the middleware?).");
		}
		return true;
	}
}