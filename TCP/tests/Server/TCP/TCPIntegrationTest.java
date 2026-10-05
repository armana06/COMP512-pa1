package Server.TCP;

import Client.TcpResourceManagerClient;
import Server.Common.ResourceManager;

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

			testCustomersAndReservations(client);
			testBundleCommitAndRollback(client);
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
