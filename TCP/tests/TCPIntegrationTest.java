import Shared.Request;
import Shared.Response;
import Shared.TcpChannel;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class TCPIntegrationTest
{
	private static int assertions;

	public static void main(String[] args) throws Exception
	{
		int flightPort = freePort();
		int carPort = freePort();
		int roomPort = freePort();
		int middlewarePort = freePort();
		List<Process> processes = new ArrayList<Process>();

		try
		{
			String classPath = absoluteClassPath(System.getProperty("java.class.path"));
			String java = new File(new File(System.getProperty("java.home"), "bin"), "java")
					.getAbsolutePath();
			processes.add(start(java, classPath, "Server.TCP.TCPResourceManagerServer",
					"Flights", Integer.toString(flightPort)));
			processes.add(start(java, classPath, "Server.TCP.TCPResourceManagerServer",
					"Cars", Integer.toString(carPort)));
			processes.add(start(java, classPath, "Server.TCP.TCPResourceManagerServer",
					"Rooms", Integer.toString(roomPort)));
			processes.add(start(java, classPath, "Server.TCP.TCPMiddlewareServer",
					"127.0.0.1", "127.0.0.1", "127.0.0.1",
					Integer.toString(flightPort), Integer.toString(carPort),
					Integer.toString(roomPort), Integer.toString(middlewarePort)));

			waitUntilReady(processes, flightPort, carPort, roomPort, middlewarePort);
			testPersistentConnection(middlewarePort);
			testResourceManagerErrorHandling(flightPort);
			testCommandRoutingAndCustomerOperations(middlewarePort);
			testBundleSuccessAndRollback(middlewarePort);
			testConcurrentReservations(middlewarePort);
			testConcurrentInventoryUpdates(middlewarePort);
			testConcurrentCustomerCreation(middlewarePort);
			testConcurrentReservationAndDeletion(middlewarePort);

			System.out.println("TCP integration tests passed (" + assertions + " assertions).");
		}
		finally
		{
			for (Process process : processes)
			{
				process.destroy();
			}
			for (Process process : processes)
			{
				if (!process.waitFor(2, java.util.concurrent.TimeUnit.SECONDS))
				{
					process.destroyForcibly();
					process.waitFor();
				}
			}
		}
	}

	private static void testResourceManagerErrorHandling(int flightPort) throws Exception
	{
		try (TcpChannel channel = connect("127.0.0.1", flightPort))
		{
			Response badArguments = exchange(channel, Request.of("queryFlight", "not-a-number"));
			check(!badArguments.isSuccess(), "RM reports invalid argument type as an error");
			check(badArguments.error.contains("ClassCastException"),
					"RM error response includes exception details");
		}
		equal("Flights", directResult(flightPort, Request.of("getName")),
				"RM remains available after a malformed request");
	}

	private static void testPersistentConnection(int middlewarePort) throws Exception
	{
		try (TcpChannel channel = connect("127.0.0.1", middlewarePort))
		{
			Response name = exchange(channel, Request.of("getName"));
			check(name.isSuccess(), "persistent connection accepts first request");
			equal("Middleware", name.result, "middleware name");

			Response query = exchange(channel, Request.of("queryFlight", 999001));
			check(query.isSuccess(), "persistent connection accepts later request");
			equal(0, query.result, "unknown flight availability is zero");

			Response unknown = exchange(channel, Request.of("notACommand"));
			check(!unknown.isSuccess(), "unknown command returns an error response");
			check(unknown.error.contains("Unknown command"), "error response explains unknown command");

			Response badArguments = exchange(channel, Request.of("queryFlight", "not-an-integer"));
			check(!badArguments.isSuccess(), "middleware returns bad argument errors");
			check(badArguments.error.contains("ClassCastException"),
					"middleware error contains argument failure details");

			Response missingArguments = exchange(channel, Request.of("queryFlight"));
			check(!missingArguments.isSuccess(), "middleware rejects requests with missing arguments");

			Response afterError = exchange(channel, Request.of("getName"));
			check(afterError.isSuccess(), "connection remains usable after command errors");
		}
	}

	private static void testCommandRoutingAndCustomerOperations(int middlewarePort) throws Exception
	{
		checkSuccess(middlewarePort, "addFlight", 1101, 2, 100);
		checkSuccess(middlewarePort, "addFlight", 1101, 1, 125);
		checkSuccess(middlewarePort, "addFlight", 1101, 0, 0);
		checkSuccess(middlewarePort, "addCars", "Montreal", 2, 70);
		checkSuccess(middlewarePort, "addCars", "Montreal", 0, 0);
		checkSuccess(middlewarePort, "addRooms", "Montreal", 1, 90);
		checkSuccess(middlewarePort, "addRooms", "Montreal", 0, 0);
		int generatedCustomerId = (Integer)result(middlewarePort, "newCustomer");
		check(generatedCustomerId > 0, "middleware generates a positive customer ID");
		checkResult(middlewarePort, true, "deleteCustomer", generatedCustomerId);

		equal(3, result(middlewarePort, "queryFlight", 1101), "flight add increments inventory");
		equal(125, result(middlewarePort, "queryFlightPrice", 1101), "flight add updates positive price");
		equal(2, result(middlewarePort, "queryCars", "Montreal"), "car inventory query routing");
		equal(70, result(middlewarePort, "queryCarsPrice", "Montreal"), "car price query routing");
		equal(1, result(middlewarePort, "queryRooms", "Montreal"), "room inventory query routing");
		equal(90, result(middlewarePort, "queryRoomsPrice", "Montreal"), "room price query routing");

		checkResult(middlewarePort, true, "newCustomer", 41001);
		checkResult(middlewarePort, false, "newCustomer", 41001);
		checkResult(middlewarePort, false, "reserveFlight", 41001, 999002);
		checkResult(middlewarePort, true, "reserveFlight", 41001, 1101);
		checkResult(middlewarePort, true, "reserveCar", 41001, "Montreal");
		checkResult(middlewarePort, true, "reserveRoom", 41001, "Montreal");
		checkResult(middlewarePort, false, "deleteFlight", 1101);
		checkResult(middlewarePort, false, "deleteCars", "Montreal");
		checkResult(middlewarePort, false, "deleteRooms", "Montreal");

		String bill = (String)result(middlewarePort, "queryCustomerInfo", 41001);
		check(bill.contains("flight-1101"), "bill includes flight reservation");
		check(bill.contains("car-montreal"), "bill includes car reservation");
		check(bill.contains("room-montreal"), "bill includes room reservation");
		check(bill.contains("Total cost: $285"), "combined bill totals all resource managers");

		checkResult(middlewarePort, true, "deleteCustomer", 41001);
		equal(3, result(middlewarePort, "queryFlight", 1101), "deleting customer restores flight");
		equal(2, result(middlewarePort, "queryCars", "Montreal"), "deleting customer restores car");
		equal(1, result(middlewarePort, "queryRooms", "Montreal"), "deleting customer restores room");
		checkResult(middlewarePort, true, "deleteFlight", 1101);
		checkResult(middlewarePort, true, "deleteCars", "Montreal");
		checkResult(middlewarePort, true, "deleteRooms", "Montreal");
		equal(0, result(middlewarePort, "queryFlightPrice", 1101), "deleted flight query returns zero");
	}

	private static void testBundleSuccessAndRollback(int middlewarePort) throws Exception
	{
		checkSuccess(middlewarePort, "addFlight", 1201, 2, 100);
		checkSuccess(middlewarePort, "addFlight", 1202, 1, 50);
		checkSuccess(middlewarePort, "addFlight", 1203, 1, 30);
		checkSuccess(middlewarePort, "addCars", "BundleTown", 1, 40);
		checkSuccess(middlewarePort, "addRooms", "BundleTown", 1, 100);
		checkSuccess(middlewarePort, "addCars", "RoomlessTown", 1, 40);
		checkSuccess(middlewarePort, "newCustomer", 42001);

		Vector<String> flights = new Vector<String>();
		flights.add("1201");
		flights.add("1202");
		checkResult(middlewarePort, true, "bundle", 42001, flights, "BundleTown", true, true);
		equal(1, result(middlewarePort, "queryFlight", 1201), "successful bundle reserves first flight");
		equal(0, result(middlewarePort, "queryFlight", 1202), "successful bundle reserves second flight");
		equal(0, result(middlewarePort, "queryCars", "BundleTown"), "successful bundle reserves car");

		Vector<String> failedFlights = new Vector<String>();
		failedFlights.add("1203");
		checkResult(middlewarePort, false, "bundle", 42001, failedFlights,
				"MissingTown", true, false);
		equal(1, result(middlewarePort, "queryFlight", 1203),
				"failed bundle restores flight reserved before unavailable car");

		Vector<String> rollbackFlights = new Vector<String>();
		rollbackFlights.add("1203");
		checkResult(middlewarePort, false, "bundle", 42001, rollbackFlights,
				"RoomlessTown", true, true);
		equal(1, result(middlewarePort, "queryFlight", 1203),
				"failed bundle restores newly reserved flight");
		equal(1, result(middlewarePort, "queryCars", "RoomlessTown"),
				"failed bundle restores newly reserved car");

		checkSuccess(middlewarePort, "addFlight", 1204, 2, 15);
		checkResult(middlewarePort, true, "reserveFlight", 42001, 1204);
		Vector<String> duplicateFlight = new Vector<String>();
		duplicateFlight.add("1204");
		duplicateFlight.add("1204");
		checkResult(middlewarePort, false, "bundle", 42001, duplicateFlight,
				"BundleTown", false, false);
		equal(1, result(middlewarePort, "queryFlight", 1204),
				"duplicate-flight bundle rollback restores only its own reservation");

		String bill = (String)result(middlewarePort, "queryCustomerInfo", 42001);
		check(bill.contains("flight-1201"), "rollback preserves customer's earlier flight");
		check(bill.contains("flight-1202"), "rollback preserves customer's other earlier flight");
		check(bill.contains("car-bundletown"), "rollback preserves customer's earlier car");
		check(!bill.contains("flight-1203"), "rollback removes only failed bundle flight");
		check(!bill.contains("car-roomlesstown"), "rollback removes failed bundle car");
		check(bill.contains("flight-1204"), "rollback keeps pre-existing reservation of same flight");
		check(bill.contains("Total cost: $305"), "bill total remains unchanged after rollback");

		checkResult(middlewarePort, true, "deleteCustomer", 42001);
		equal(2, result(middlewarePort, "queryFlight", 1201), "delete restores bundled flight");
		equal(1, result(middlewarePort, "queryFlight", 1202), "delete restores second bundled flight");
		equal(1, result(middlewarePort, "queryCars", "BundleTown"), "delete restores bundled car");
	}

	private static void testConcurrentReservations(int middlewarePort) throws Exception
	{
		checkSuccess(middlewarePort, "addFlight", 1301, 12, 10);
		int clients = 8;
		int attemptsPerClient = 10;
		for (int i = 0; i < clients; ++i)
		{
			checkSuccess(middlewarePort, "newCustomer", 43000 + i);
		}

		ExecutorService workers = Executors.newFixedThreadPool(clients);
		try
		{
			List<Future<Integer>> results = new ArrayList<Future<Integer>>();
			for (int i = 0; i < clients; ++i)
			{
				final int customerId = 43000 + i;
				results.add(workers.submit(new Callable<Integer>()
				{
					public Integer call() throws Exception
					{
						int successes = 0;
						for (int attempt = 0; attempt < attemptsPerClient; ++attempt)
						{
							if (Boolean.TRUE.equals(result(middlewarePort,
									"reserveFlight", customerId, 1301)))
							{
								++successes;
							}
						}
						return successes;
					}
				}));
			}

			int successes = 0;
			for (Future<Integer> result : results)
			{
				successes += result.get();
			}
			equal(12, successes, "concurrent clients reserve exactly available inventory");
			equal(0, result(middlewarePort, "queryFlight", 1301),
					"concurrent reservations never oversell inventory");

			int billedReservations = 0;
			for (int i = 0; i < clients; ++i)
			{
				String bill = (String)result(middlewarePort,
						"queryCustomerInfo", 43000 + i);
				billedReservations += reservationCount(bill, "flight-1301");
			}
			equal(successes, billedReservations,
					"inventory reservations agree with customer bills");

			for (int i = 0; i < clients; ++i)
			{
				checkResult(middlewarePort, true, "deleteCustomer", 43000 + i);
			}
			equal(12, result(middlewarePort, "queryFlight", 1301),
					"deleting concurrent-test customers restores all seats");
		}
		finally
		{
			workers.shutdownNow();
		}
	}

	private static void testConcurrentInventoryUpdates(int middlewarePort) throws Exception
	{
		int workersCount = 6;
		int additionsPerWorker = 25;
		ExecutorService workers = Executors.newFixedThreadPool(workersCount);
		CountDownLatch ready = new CountDownLatch(workersCount);
		CountDownLatch start = new CountDownLatch(1);
		try
		{
			List<Future<?>> additions = new ArrayList<Future<?>>();
			for (int i = 0; i < workersCount; ++i)
			{
				final int price = 10 + i;
				additions.add(workers.submit(new Callable<Void>()
				{
					public Void call() throws Exception
					{
						ready.countDown();
						start.await();
						for (int j = 0; j < additionsPerWorker; ++j)
						{
							checkResult(middlewarePort, true, "addFlight", 1401, 1, price);
							checkResult(middlewarePort, true, "addCars", "ConcurrentTown", 1, price);
							checkResult(middlewarePort, true, "addRooms", "ConcurrentTown", 1, price);
						}
						return null;
					}
				}));
			}

			ready.await();
			start.countDown();
			for (Future<?> addition : additions)
			{
				addition.get();
			}

			int totalAdditions = workersCount * additionsPerWorker;
			equal(totalAdditions, result(middlewarePort, "queryFlight", 1401),
					"concurrent flight additions are not lost");
			equal(totalAdditions, result(middlewarePort, "queryCars", "ConcurrentTown"),
					"concurrent car additions are not lost");
			equal(totalAdditions, result(middlewarePort, "queryRooms", "ConcurrentTown"),
					"concurrent room additions are not lost");
			check((Integer)result(middlewarePort, "queryFlightPrice", 1401) >= 10,
					"flight keeps a positive price after concurrent additions");
			check((Integer)result(middlewarePort, "queryCarsPrice", "ConcurrentTown") >= 10,
					"car keeps a positive price after concurrent additions");
			check((Integer)result(middlewarePort, "queryRoomsPrice", "ConcurrentTown") >= 10,
					"room keeps a positive price after concurrent additions");
		}
		finally
		{
			start.countDown();
			workers.shutdownNow();
		}

		checkSuccess(middlewarePort, "deleteFlight", 1401);
		checkSuccess(middlewarePort, "deleteCars", "ConcurrentTown");
		checkSuccess(middlewarePort, "deleteRooms", "ConcurrentTown");
	}

	private static void testConcurrentCustomerCreation(int middlewarePort) throws Exception
	{
		int count = 16;
		ExecutorService workers = Executors.newFixedThreadPool(count);
		CountDownLatch ready = new CountDownLatch(count);
		CountDownLatch start = new CountDownLatch(1);
		try
		{
			List<Future<Integer>> ids = new ArrayList<Future<Integer>>();
			for (int i = 0; i < count; ++i)
			{
				ids.add(workers.submit(new Callable<Integer>()
				{
					public Integer call() throws Exception
					{
						ready.countDown();
						start.await();
						return (Integer)result(middlewarePort, "newCustomer");
					}
				}));
			}

			ready.await();
			start.countDown();
			Set<Integer> uniqueIds = new HashSet<Integer>();
			for (Future<Integer> id : ids)
			{
				uniqueIds.add(id.get());
			}
			equal(count, uniqueIds.size(), "concurrent generated customer IDs are unique");
			for (Integer customerId : uniqueIds)
			{
				String bill = (String)result(middlewarePort, "queryCustomerInfo", customerId);
				check(bill.startsWith("Bill for customer " + customerId),
						"generated customer is replicated across all resource managers");
				checkResult(middlewarePort, true, "deleteCustomer", customerId);
			}
		}
		finally
		{
			start.countDown();
			workers.shutdownNow();
		}
	}

	private static void testConcurrentReservationAndDeletion(int middlewarePort) throws Exception
	{
		int reservationWorkers = 12;
		for (int iteration = 0; iteration < 5; ++iteration)
		{
			int flightNumber = 1501 + iteration;
			int customerId = 45001 + iteration;
			checkSuccess(middlewarePort, "addFlight", flightNumber, 1, 25);
			checkSuccess(middlewarePort, "newCustomer", customerId);

			ExecutorService workers = Executors.newFixedThreadPool(reservationWorkers + 1);
			CountDownLatch ready = new CountDownLatch(reservationWorkers + 1);
			CountDownLatch start = new CountDownLatch(1);
			try
			{
				List<Future<Boolean>> reservations = new ArrayList<Future<Boolean>>();
				for (int i = 0; i < reservationWorkers; ++i)
				{
					reservations.add(workers.submit(new Callable<Boolean>()
					{
						public Boolean call() throws Exception
						{
							ready.countDown();
							start.await();
							return (Boolean)result(middlewarePort,
									"reserveFlight", customerId, flightNumber);
						}
					}));
				}
				Future<Boolean> deletion = workers.submit(new Callable<Boolean>()
				{
					public Boolean call() throws Exception
					{
						ready.countDown();
						start.await();
						return (Boolean)result(middlewarePort, "deleteCustomer", customerId);
					}
				});

				ready.await();
				start.countDown();
				int successfulReservations = 0;
				for (Future<Boolean> reservation : reservations)
				{
					if (reservation.get())
					{
						++successfulReservations;
					}
				}
				check(deletion.get(), "concurrent customer deletion completes");
				check(successfulReservations <= 1,
						"concurrent reserve/delete race never oversells the item");
				equal(1, result(middlewarePort, "queryFlight", flightNumber),
						"concurrent deletion restores any racing reservation");
				equal("Bill for customer " + customerId + "\nTotal cost: $0\n",
						result(middlewarePort, "queryCustomerInfo", customerId),
						"deleted customer has no remaining reservations");
			}
			finally
			{
				start.countDown();
				workers.shutdownNow();
			}

			checkSuccess(middlewarePort, "deleteFlight", flightNumber);
		}
	}

	private static int reservationCount(String bill, String item)
	{
		for (String line : bill.split("\n"))
		{
			if (line.endsWith(item + " $10"))
			{
				return Integer.parseInt(line.substring(0, line.indexOf(' ')));
			}
		}
		return 0;
	}

	private static void waitUntilReady(List<Process> processes, int flightPort, int carPort,
			int roomPort, int middlewarePort) throws Exception
	{
		Exception lastFailure = null;
		for (int attempt = 0; attempt < 50; ++attempt)
		{
			for (Process process : processes)
			{
				if (!process.isAlive())
				{
					throw new AssertionError("A test server exited during startup");
				}
			}
			try
			{
				if ("Flights".equals(directResult(flightPort, Request.of("getName")))
						&& "Cars".equals(directResult(carPort, Request.of("getName")))
						&& "Rooms".equals(directResult(roomPort, Request.of("getName")))
						&& "Middleware".equals(result(middlewarePort, "getName")))
				{
					return;
				}
			}
			catch (Exception e)
			{
				lastFailure = e;
				Thread.sleep(100);
			}
		}
		throw new AssertionError("TCP test servers did not become ready", lastFailure);
	}

	private static Process start(String java, String classPath, String mainClass, String... args)
			throws IOException
	{
		List<String> command = new ArrayList<String>();
		command.add(java);
		command.add("-cp");
		command.add(classPath);
		command.add(mainClass);
		for (String arg : args)
		{
			command.add(arg);
		}
		return new ProcessBuilder(command).redirectErrorStream(true)
				.redirectOutput(ProcessBuilder.Redirect.INHERIT).start();
	}

	private static String absoluteClassPath(String classPath)
	{
		String[] entries = classPath.split(File.pathSeparator);
		StringBuilder absolute = new StringBuilder();
		for (String entry : entries)
		{
			if (absolute.length() > 0)
			{
				absolute.append(File.pathSeparator);
			}
			absolute.append(new File(entry).getAbsolutePath());
		}
		return absolute.toString();
	}

	private static int freePort() throws IOException
	{
		try (ServerSocket socket = new ServerSocket(0))
		{
			return socket.getLocalPort();
		}
	}

	private static TcpChannel connect(String host, int port) throws IOException
	{
		Socket socket = new Socket(host, port);
		socket.setSoTimeout(5000);
		return new TcpChannel(socket);
	}

	private static Response exchange(TcpChannel channel, Request request) throws Exception
	{
		channel.sendRequest(request);
		return channel.receiveResponse();
	}

	private static Object directResult(int port, Request request) throws Exception
	{
		return send("127.0.0.1", port, request);
	}

	private static Object result(int middlewarePort, String command, Object... args)
			throws Exception
	{
		return send("127.0.0.1", middlewarePort, Request.of(command, args));
	}

	private static Object send(String host, int port, Request request) throws Exception
	{
		try (TcpChannel channel = connect(host, port))
		{
			Response response = exchange(channel, request);
			if (!response.isSuccess())
			{
				throw new AssertionError("Request " + request.command + " failed: " + response.error);
			}
			return response.result;
		}
	}

	private static void checkSuccess(int port, String command, Object... args) throws Exception
	{
		check(Boolean.TRUE.equals(result(port, command, args)),
				command + " should succeed");
	}

	private static void checkResult(int port, boolean expected, String command, Object... args)
			throws Exception
	{
		equal(expected, result(port, command, args), command + " result");
	}

	private static synchronized void check(boolean condition, String message)
	{
		++assertions;
		if (!condition)
		{
			throw new AssertionError(message);
		}
	}

	private static synchronized void equal(Object expected, Object actual, String message)
	{
		++assertions;
		if (expected == null ? actual != null : !expected.equals(actual))
		{
			throw new AssertionError(message + ": expected <" + expected
					+ ">, got <" + actual + ">");
		}
	}
}
