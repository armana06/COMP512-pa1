package Server.TCP;

import Client.TcpResourceManagerClient;
import Server.Common.ResourceManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class TCPIntegrationTest
{
	private TCPIntegrationTest()
	{
	}

	public static void main(String[] args) throws Exception
	{
		SlowResourceManager flightManager = new SlowResourceManager();
		TCPResourceManagerServer flights =
			new TCPResourceManagerServer(flightManager, 0);
		TCPResourceManagerServer cars =
			new TCPResourceManagerServer(new ResourceManager("Cars"), 0);
		TCPResourceManagerServer rooms =
			new TCPResourceManagerServer(new ResourceManager("Rooms"), 0);
		TCPMiddlewareServer middleware = null;
		try
		{
			int flightPort = flights.start();
			int carPort = cars.start();
			int roomPort = rooms.start();
			Map<ResourceType, TCPMiddlewareServer.Endpoint> endpoints =
				new EnumMap<ResourceType, TCPMiddlewareServer.Endpoint>(ResourceType.class);
			endpoints.put(ResourceType.FLIGHT,
				new TCPMiddlewareServer.Endpoint("127.0.0.1", flightPort));
			endpoints.put(ResourceType.CAR,
				new TCPMiddlewareServer.Endpoint("127.0.0.1", carPort));
			endpoints.put(ResourceType.ROOM,
				new TCPMiddlewareServer.Endpoint("127.0.0.1", roomPort));
			middleware = new TCPMiddlewareServer(endpoints, 0);
			int middlewarePort = middleware.start();
			TcpResourceManagerClient client =
				new TcpResourceManagerClient("127.0.0.1", middlewarePort);

			testProtocolCodec();
			testInvalidRequests(middlewarePort);
			testCustomersAndReservations(client);
			testReservationEdgeCases(client);
			testBundleCommitAndRollback(client);
			testAdditionalBundlePaths(client);
			testConcurrentRequests(client, flightManager);

			System.out.println("TCP integration tests passed");
		}
		finally
		{
			if (middleware != null)
			{
				middleware.close();
			}
			flights.close();
			cars.close();
			rooms.close();
		}
	}

	private static void testProtocolCodec() throws Exception
	{
		Vector<String> strings = new Vector<String>(Arrays.asList("101", "202"));
		Object[] arguments = new Object[] {
			Integer.valueOf(42), Boolean.TRUE, "Montreal", strings, null
		};
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		TcpProtocol.writeRequest(new DataOutputStream(bytes),
			new TcpProtocol.Request(17, "bundle", arguments));
		TcpProtocol.Request decoded = TcpProtocol.readRequest(
			new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
		check(decoded.id == 17 && decoded.method.equals("bundle"),
			"request protocol preserves request identity");
		check(decoded.arguments.length == 5 &&
			Integer.valueOf(42).equals(decoded.arguments[0]) &&
			Boolean.TRUE.equals(decoded.arguments[1]) &&
			"Montreal".equals(decoded.arguments[2]) &&
			strings.equals(decoded.arguments[3]) && decoded.arguments[4] == null,
			"request protocol round-trips every supported value type");

		bytes.reset();
		TcpProtocol.writeResponse(new DataOutputStream(bytes),
			TcpProtocol.Response.failure(18, "invalid request"));
		TcpProtocol.Response failure = TcpProtocol.readResponse(
			new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
		check(failure.id == 18 && "invalid request".equals(failure.error),
			"response protocol round-trips errors");

		bytes.reset();
		TcpProtocol.writeResponse(new DataOutputStream(bytes),
			TcpProtocol.Response.success(19, Integer.valueOf(123)));
		TcpProtocol.Response success = TcpProtocol.readResponse(
			new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
		check(success.id == 19 && Integer.valueOf(123).equals(success.value),
			"response protocol round-trips successful values");

		ByteArrayOutputStream malformed = new ByteArrayOutputStream();
		DataOutputStream malformedOutput = new DataOutputStream(malformed);
		malformedOutput.writeLong(20);
		malformedOutput.writeUTF("bad");
		malformedOutput.writeInt(65);
		expectIOException(() -> TcpProtocol.readRequest(
			new DataInputStream(new ByteArrayInputStream(malformed.toByteArray()))),
			"request protocol rejects excessive argument counts");

		malformed.reset();
		malformedOutput = new DataOutputStream(malformed);
		malformedOutput.writeLong(21);
		malformedOutput.writeBoolean(false);
		malformedOutput.writeByte(99);
		malformedOutput.flush();
		expectIOException(() -> TcpProtocol.readResponse(
			new DataInputStream(new ByteArrayInputStream(malformed.toByteArray()))),
			"response protocol rejects unknown value tags");
	}

	private static void testInvalidRequests(int middlewarePort) throws Exception
	{
		TcpProtocol.Response unknown = rawRequest(middlewarePort,
			new TcpProtocol.Request(31, "notAResourceMethod", new Object[0]));
		check(unknown.error != null && unknown.error.contains("Unsupported middleware method"),
			"middleware rejects unknown methods with an error response");

		TcpProtocol.Response wrongArity = rawRequest(middlewarePort,
			new TcpProtocol.Request(32, "queryFlight", new Object[0]));
		check(wrongArity.error != null && wrongArity.error.contains("argument count"),
			"ResourceManager rejects incorrect method argument counts");

		TcpProtocol.Response wrongType = rawRequest(middlewarePort,
			new TcpProtocol.Request(33, "queryFlight", new Object[] { "not-an-integer" }));
		check(wrongType.error != null && wrongType.error.contains("Expected integer"),
			"ResourceManager rejects incorrectly typed method arguments");

		TcpProtocol.Response localWrongArity = rawRequest(middlewarePort,
			new TcpProtocol.Request(34, "getName", new Object[] { "unexpected" }));
		check(localWrongArity.error != null && localWrongArity.error.contains("argument count"),
			"middleware validates locally handled method argument counts");

		TcpProtocol.Response emptyBundle = rawRequest(middlewarePort,
			new TcpProtocol.Request(35, "bundle", new Object[] {
				Integer.valueOf(7001), new Vector<String>(), "Montreal",
				Boolean.FALSE, Boolean.FALSE
			}));
		check(emptyBundle.error != null && emptyBundle.error.contains("at least one flight"),
			"middleware rejects bundles without a flight");

		TcpResourceManagerClient client = new TcpResourceManagerClient("127.0.0.1", middlewarePort);
		check(client.getName().equals("Middleware"),
			"middleware continues serving valid requests after malformed requests");
	}

	private static void testCustomersAndReservations(TcpResourceManagerClient client)
		throws RemoteException
	{
		check("Middleware".equals(client.getName()), "middleware name");
		check(client.newCustomer(7001), "create customer");
		check(!client.newCustomer(7001), "reject duplicate customer ID");

		int generatedId = client.newCustomer();
		check(generatedId > 0, "generate customer ID");
		check(!client.newCustomer(generatedId), "generated customer replicated");

		check(client.addFlight(100, 2, 100), "add flight");
		check(client.addFlight(100, 3, 125), "extend flight and update price");
		check(client.queryFlight(100) == 5, "query updated flight availability");
		check(client.queryFlightPrice(100) == 125, "query updated flight price");
		check(client.addCars("Montreal", 2, 80), "add cars");
		check(client.addRooms("Montreal", 3, 50), "add rooms");
		check(client.queryCars("Montreal") == 2 && client.queryCarsPrice("Montreal") == 80,
			"query cars");
		check(client.queryRooms("Montreal") == 3 && client.queryRoomsPrice("Montreal") == 50,
			"query rooms");

		check(client.reserveFlight(7001, 100), "reserve flight");
		check(client.reserveCar(7001, "Montreal"), "reserve car");
		check(client.reserveRoom(7001, "Montreal"), "reserve room");
		check(client.queryFlight(100) == 4, "reserved seat removed from availability");
		check(!client.deleteFlight(100), "cannot delete a reserved flight");
		check(!client.deleteCars("Montreal") && !client.deleteRooms("Montreal"),
			"cannot delete reserved cars or rooms");
		String bill = client.queryCustomerInfo(7001);
		check(bill.contains("flight-100") && bill.contains("car-montreal") &&
			bill.contains("room-montreal"), "customer bill includes each reservation");
		check(bill.contains("Total cost: $255"), "customer bill contains total cost");
		check(client.deleteCustomer(7001), "delete customer");
		check(client.queryFlight(100) == 5 && client.queryCars("Montreal") == 2 &&
			client.queryRooms("Montreal") == 3, "customer deletion releases reservations");
		check(client.queryCustomerInfo(7001).isEmpty(), "deleted customer has no bill");
		check(client.deleteFlight(100), "delete unreserved flight");
		check(client.deleteCars("Montreal") && client.deleteRooms("Montreal"),
			"delete unreserved cars and rooms");
		check(client.queryFlight(100) == 0, "deleted flight is not found");
		check(client.queryCars("Montreal") == 0 && client.queryRooms("Montreal") == 0,
			"deleted cars and rooms are not found");
		check(!client.deleteCustomer(7001), "deleting an unknown customer returns false");
		check(client.queryFlight(987654) == 0 && client.queryCars("Nowhere") == 0 &&
			client.queryRooms("Nowhere") == 0, "queries for missing resources return zero");
		check(client.queryFlightPrice(987654) == 0 && client.queryCarsPrice("Nowhere") == 0 &&
			client.queryRoomsPrice("Nowhere") == 0, "missing resource prices return zero");
	}

	private static void testReservationEdgeCases(TcpResourceManagerClient client)
		throws RemoteException
	{
		check(client.newCustomer(7100), "create reservation-edge customer");
		check(!client.reserveFlight(7100, 710), "cannot reserve a missing flight");
		check(!client.reserveCar(7100, "Missing") && !client.reserveRoom(7100, "Missing"),
			"cannot reserve missing cars or rooms");
		check(client.addFlight(710, 2, 30), "add repeated-reservation flight");
		check(client.addCars("Repeat", 2, 40), "add repeated-reservation cars");
		check(client.addRooms("Repeat", 2, 50), "add repeated-reservation rooms");
		check(!client.reserveFlight(999999, 710) &&
			!client.reserveCar(999999, "Repeat") &&
			!client.reserveRoom(999999, "Repeat"),
			"cannot reserve resources for a nonexistent customer");

		check(client.reserveFlight(7100, 710) && client.reserveFlight(7100, 710),
			"customer can reserve multiple seats on one flight");
		check(client.reserveCar(7100, "Repeat") && client.reserveCar(7100, "Repeat"),
			"customer can reserve multiple cars at one location");
		check(client.reserveRoom(7100, "Repeat") && client.reserveRoom(7100, "Repeat"),
			"customer can reserve multiple rooms at one location");
		check(client.queryFlight(710) == 0 && client.queryCars("Repeat") == 0 &&
			client.queryRooms("Repeat") == 0, "multiple reservations consume exact inventory");
		String repeatedBill = client.queryCustomerInfo(7100);
		check(repeatedBill.contains("2 flight-710") && repeatedBill.contains("2 car-repeat") &&
			repeatedBill.contains("2 room-repeat"), "bill aggregates repeated reservations");
		check(repeatedBill.contains("Total cost: $240"),
			"bill total includes all quantities and prices");
		check(client.deleteCustomer(7100), "delete repeated-reservation customer");
		check(client.queryFlight(710) == 2 && client.queryCars("Repeat") == 2 &&
			client.queryRooms("Repeat") == 2, "deleting customer releases multiple reservations");

		check(client.addCars("Repeat", 1, 0) && client.addRooms("Repeat", 1, -5),
			"add inventory with nonpositive replacement price");
		check(client.queryCarsPrice("Repeat") == 40 && client.queryRoomsPrice("Repeat") == 50,
			"nonpositive replacement prices preserve existing prices");

		check(client.newCustomer(7101), "create stock-limit customer");
		check(client.addFlight(711, 1, 10), "add single-seat flight");
		check(client.reserveFlight(7101, 711), "reserve the only available seat");
		check(!client.reserveFlight(7101, 711), "cannot reserve beyond available inventory");
		check(client.queryFlight(711) == 0, "failed reservation does not overdraw inventory");
	}

	private static void testBundleCommitAndRollback(TcpResourceManagerClient client)
		throws RemoteException
	{
		check(client.newCustomer(7002), "create bundle customer");
		check(client.addFlight(201, 1, 200), "add first bundle flight");
		check(client.addFlight(202, 1, 300), "add second bundle flight");
		check(client.addCars("Quebec", 1, 75), "add bundle car");
		check(client.addRooms("Quebec", 1, 125), "add bundle room");

		Vector<String> flights = new Vector<String>(Arrays.asList("201", "202"));
		check(client.bundle(7002, flights, "Quebec", true, true), "complete bundle");
		check(client.queryFlight(201) == 0 && client.queryFlight(202) == 0 &&
			client.queryCars("Quebec") == 0 && client.queryRooms("Quebec") == 0,
			"bundle reserves each requested item");
		String bill = client.queryCustomerInfo(7002);
		check(bill.contains("flight-201") && bill.contains("flight-202") &&
			bill.contains("car-quebec") && bill.contains("room-quebec"),
			"bundle is reflected in customer bill");

		check(client.newCustomer(7003), "create rollback customer");
		check(client.addFlight(203, 1, 150), "add rollback flight");
		check(client.addRooms("SoldOut", 0, 90), "add sold-out room location");
		Vector<String> failingFlights = new Vector<String>(Arrays.asList("203"));
		check(!client.bundle(7003, failingFlights, "SoldOut", false, true),
			"reject bundle with unavailable room");
		check(client.queryFlight(203) == 1, "failed bundle restores reserved flight");
		check(!client.queryCustomerInfo(7003).contains("flight-203"),
			"failed bundle leaves no customer reservation behind");
	}

	private static void testAdditionalBundlePaths(TcpResourceManagerClient client)
		throws RemoteException
	{
		check(client.newCustomer(7200), "create flights-only bundle customer");
		check(client.addFlight(204, 2, 60), "add repeated-flight bundle inventory");
		check(client.bundle(7200, new Vector<String>(Arrays.asList("204", "204")),
			"Unused", false, false), "reserve repeated flights without car or room");
		check(client.queryFlight(204) == 0, "flights-only bundle consumes requested seats");
		check(client.queryCustomerInfo(7200).contains("2 flight-204"),
			"repeated flight bundle is aggregated in bill");

		check(client.newCustomer(7201), "create multi-step rollback customer");
		check(client.addFlight(205, 1, 70) && client.addFlight(206, 0, 80),
			"add partial-failure bundle flights");
		check(!client.bundle(7201, new Vector<String>(Arrays.asList("205", "206")),
			"Unused", false, false), "bundle fails when a later flight has no inventory");
		check(client.queryFlight(205) == 1 &&
			!client.queryCustomerInfo(7201).contains("flight-205"),
			"multi-step bundle rollback releases earlier flight reservations");

		check(client.newCustomer(7202), "create car rollback customer");
		check(client.addFlight(207, 1, 90) && client.addCars("CarSoldOut", 0, 20),
			"add bundle flight and sold-out car");
		check(!client.bundle(7202, new Vector<String>(Arrays.asList("207")),
			"CarSoldOut", true, false), "bundle fails when optional car is unavailable");
		check(client.queryFlight(207) == 1 &&
			!client.queryCustomerInfo(7202).contains("flight-207"),
			"failed car reservation rolls back earlier flight");
	}

	private static void testConcurrentRequests(TcpResourceManagerClient client,
		SlowResourceManager flightManager) throws Exception
	{
		final int requestCount = 32;
		ExecutorService callers = Executors.newFixedThreadPool(8);
		try
		{
			List<Future<Boolean>> results = new ArrayList<Future<Boolean>>(requestCount);
			for (int i = 0; i < requestCount; i++)
			{
				results.add(callers.submit(() -> client.addFlight(999, 1, 10)));
			}
			for (Future<Boolean> result : results)
			{
				check(result.get().booleanValue(), "concurrent add request");
			}
			check(client.queryFlight(999) == requestCount,
				"concurrent requests are not lost");

			check(client.addFlight(1000, 1, 10), "add slow-query test flight");
			Future<Integer> slowQuery = callers.submit(() -> client.queryFlight(1000));
			check(flightManager.queryStarted.await(2, TimeUnit.SECONDS),
				"slow flight query reached ResourceManager");
			Future<Boolean> sameManagerRequest = callers.submit(
				() -> client.addFlight(1001, 1, 10));
			Future<Boolean> independentRequest = callers.submit(
				() -> client.addCars("Concurrent", 1, 10));
			check(sameManagerRequest.get(1, TimeUnit.SECONDS).booleanValue(),
				"ResourceManager processes another request while one is running");
			check(independentRequest.get(1, TimeUnit.SECONDS).booleanValue(),
				"middleware accepts other work while a ResourceManager is busy");
			check(slowQuery.get(3, TimeUnit.SECONDS).intValue() == 1,
				"slow request receives its matching response");

			check(client.newCustomer(7300), "create concurrent reservation customer");
			check(client.addFlight(7300, 1, 15), "add inventory for concurrent reservation race");
			List<Future<Boolean>> reservations = new ArrayList<Future<Boolean>>();
			for (int i = 0; i < requestCount; i++)
			{
				reservations.add(callers.submit(() -> client.reserveFlight(7300, 7300)));
			}
			int successfulReservations = 0;
			for (Future<Boolean> reservation : reservations)
			{
				if (reservation.get().booleanValue())
				{
					successfulReservations++;
				}
			}
			check(successfulReservations == 1 && client.queryFlight(7300) == 0,
				"concurrent reservations cannot oversell a single seat");
			check(client.deleteCustomer(7300) && client.queryFlight(7300) == 1,
				"concurrent reservation can be released by customer deletion");

			expectRemoteException(() -> client.queryFlight(888888),
				"ResourceManager failures are returned to the client");
			check(client.queryFlight(999) == requestCount,
				"ResourceManager continues serving after an operation throws");
		}
		finally
		{
			callers.shutdownNow();
		}
	}

	private static void check(boolean condition, String description)
	{
		if (!condition)
		{
			throw new AssertionError("Failed: " + description);
		}
	}

	private static TcpProtocol.Response rawRequest(int port, TcpProtocol.Request request)
		throws IOException
	{
		try (Socket socket = new Socket())
		{
			socket.connect(new InetSocketAddress("127.0.0.1", port), 2000);
			socket.setSoTimeout(5000);
			DataOutputStream output = new DataOutputStream(socket.getOutputStream());
			DataInputStream input = new DataInputStream(socket.getInputStream());
			TcpProtocol.writeRequest(output, request);
			TcpProtocol.Response response = TcpProtocol.readResponse(input);
			check(response.id == request.id, "response ID matches raw request");
			return response;
		}
	}

	private static void expectIOException(IoAction action, String description) throws Exception
	{
		try
		{
			action.run();
			throw new AssertionError("Failed: " + description);
		}
		catch (IOException expected)
		{
		}
	}

	private static void expectRemoteException(RemoteAction action, String description)
		throws Exception
	{
		try
		{
			action.run();
			throw new AssertionError("Failed: " + description);
		}
		catch (RemoteException expected)
		{
		}
	}

	private interface IoAction
	{
		void run() throws Exception;
	}

	private interface RemoteAction
	{
		void run() throws RemoteException;
	}

	private static final class SlowResourceManager extends ResourceManager
	{
		final CountDownLatch queryStarted = new CountDownLatch(1);

		SlowResourceManager()
		{
			super("Flights");
		}

		@Override
		public int queryFlight(int flightNumber) throws RemoteException
		{
			if (flightNumber == 888888)
			{
				throw new RemoteException("Injected ResourceManager failure");
			}
			if (flightNumber == 1000)
			{
				queryStarted.countDown();
			}
			try
			{
				Thread.sleep(2500);
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
				throw new RemoteException("Flight query interrupted", e);
			}
			return super.queryFlight(flightNumber);
		}
	}
}
