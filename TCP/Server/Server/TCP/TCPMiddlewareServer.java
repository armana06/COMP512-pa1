package Server.TCP;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class TCPMiddlewareServer implements AutoCloseable
{
	private static final int CONNECT_TIMEOUT_MILLIS = 5000;

	private final EnumMap<ResourceType, BackendLink> backends =
		new EnumMap<ResourceType, BackendLink>(ResourceType.class);
	private final ConcurrentMap<Long, PendingCall> pending =
		new ConcurrentHashMap<Long, PendingCall>();
	private final Set<Socket> clientSockets = ConcurrentHashMap.newKeySet();
	private final ExecutorService clientHandlers = Executors.newCachedThreadPool();
	private final AtomicLong requestIds = new AtomicLong();
	private final AtomicInteger generatedCustomerIds = new AtomicInteger(100000);
	private final AtomicBoolean running = new AtomicBoolean();
	private final int requestedPort;
	private ServerSocket listener;

	public static final class Endpoint
	{
		public final String host;
		public final int port;

		public Endpoint(String host, int port)
		{
			if (host == null || host.trim().isEmpty())
			{
				throw new IllegalArgumentException("Backend host cannot be empty");
			}
			if (port < 1 || port > 65535)
			{
				throw new IllegalArgumentException("Backend port must be between 1 and 65535");
			}
			this.host = host;
			this.port = port;
		}
	}

	public TCPMiddlewareServer(Map<ResourceType, Endpoint> endpoints, int port) throws IOException
	{
		EnumMap<ResourceType, Endpoint> configuredEndpoints =
			new EnumMap<ResourceType, Endpoint>(endpoints);
		if (configuredEndpoints.size() != ResourceType.values().length)
		{
			throw new IllegalArgumentException("An endpoint is required for Flights, Cars, and Rooms");
		}
		if (port < 0 || port > 65535)
		{
			throw new IllegalArgumentException("Middleware port must be between 0 and 65535");
		}
		this.requestedPort = port;

		try
		{
			for (ResourceType type : ResourceType.values())
			{
				Endpoint endpoint = configuredEndpoints.get(type);
				if (endpoint == null)
				{
					throw new IllegalArgumentException("Missing endpoint for " + type.name);
				}
				backends.put(type, new BackendLink(type, endpoint));
			}
		}
		catch (IOException | RuntimeException e)
		{
			closeBackends();
			throw e;
		}
	}

	public synchronized int start() throws IOException
	{
		if (running.get())
		{
			return listener.getLocalPort();
		}
		listener = new ServerSocket(requestedPort);
		running.set(true);
		for (BackendLink backend : backends.values())
		{
			backend.startReader();
		}
		Thread acceptor = new Thread(this::acceptClients, "tcp-middleware-accept");
		acceptor.setDaemon(true);
		acceptor.start();
		return listener.getLocalPort();
	}

	private void acceptClients()
	{
		while (running.get())
		{
			try
			{
				Socket socket = listener.accept();
				clientSockets.add(socket);
				clientHandlers.execute(() -> receiveClientRequest(socket));
			}
			catch (IOException e)
			{
				if (running.get())
				{
					System.err.println("Middleware accept failed: " + e.getMessage());
				}
			}
		}
	}

	private void receiveClientRequest(Socket socket)
	{
		ClientReply reply = null;
		try
		{
			socket.setSoTimeout(30000);
			DataInputStream input = new DataInputStream(socket.getInputStream());
			DataOutputStream output = new DataOutputStream(socket.getOutputStream());
			TcpProtocol.Request request = TcpProtocol.readRequest(input);
			reply = new ClientReply(request.id, socket, output);
			route(request, reply);
		}
		catch (EOFException e)
		{
			closeClient(socket);
		}
		catch (Exception e)
		{
			if (reply == null)
			{
				try
				{
					reply = new ClientReply(0, socket, new DataOutputStream(socket.getOutputStream()));
				}
				catch (IOException ignored)
				{
					closeClient(socket);
					return;
				}
			}
			reply.failure(errorMessage(e));
		}
	}

	private void route(TcpProtocol.Request request, ClientReply reply)
	{
		String method = request.method;
		Object[] args = request.arguments;
		if ("getName".equals(method))
		{
			requireCount(request, 0);
			reply.success("Middleware");
		}
		else if ("bundle".equals(method))
		{
			try
			{
				requireCount(request, 5);
				new BundleExecution(
					integer(args[0]), stringVector(args[1]), string(args[2]),
					bool(args[3]), bool(args[4]), reply).advance();
			}
			catch (Exception e)
			{
				reply.failure(errorMessage(e));
			}
		}
		else if ("newCustomer".equals(method) && args.length == 0)
		{
			createGeneratedCustomer(reply);
		}
		else if ("newCustomer".equals(method) && args.length == 1)
		{
			routeManualCustomer(request, reply);
		}
		else if ("deleteCustomer".equals(method))
		{
			requireCount(request, 1);
			dispatchToAll(request, result -> reply.success(allTrue(result)));
		}
		else if ("queryCustomerInfo".equals(method))
		{
			requireCount(request, 1);
			dispatchToAll(request, result -> {
				String error = firstError(result);
				if (error != null)
				{
					reply.failure(error);
				}
				else
				{
					try
					{
						reply.success(mergeBills(integer(args[0]), result));
					}
					catch (Exception e)
					{
						reply.failure(errorMessage(e));
					}
				}
			});
		}
		else
		{
			ResourceType type = resourceFor(method);
			if (type == null)
			{
				reply.failure("Unsupported middleware method: " + method);
				return;
			}
			dispatchTo(type, method, args, response -> sendClientResponse(reply, response));
		}
	}

	private void routeManualCustomer(TcpProtocol.Request request, ClientReply reply)
	{
		try
		{
			requireCount(request, 1);
			final int customerId = integer(request.arguments[0]);
			dispatchToAll(request, result -> {
				String error = firstError(result);
				if (error != null)
				{
					cleanupPartialCustomer(customerId, successfulTargets(result), cleanup -> {
						String cleanupError = firstError(cleanup);
						reply.failure(cleanupError == null ? error :
							error + "; partial customer cleanup failed: " + cleanupError);
					});
				}
				else if (allTrue(result))
				{
					reply.success(Boolean.TRUE);
				}
				else
				{
					cleanupPartialCustomer(customerId, successfulTargets(result), cleanup -> {
						if (firstError(cleanup) != null ||
							!allTargetsTrue(cleanup, successfulTargets(result)))
						{
							reply.failure("Customer creation failed and partial registration cleanup failed");
						}
						else
						{
							reply.success(Boolean.FALSE);
						}
					});
				}
			});
		}
		catch (Exception e)
		{
			reply.failure(errorMessage(e));
		}
	}

	private void createGeneratedCustomer(ClientReply reply)
	{
		int customerId = generatedCustomerIds.getAndIncrement();
		if (customerId <= 0)
		{
			reply.failure("Customer ID space exhausted");
			return;
		}
		dispatchToAll("newCustomer", new Object[] { Integer.valueOf(customerId) }, result -> {
			String error = firstError(result);
			if (error != null)
			{
				cleanupPartialCustomer(customerId, successfulTargets(result), cleanup -> {
					String cleanupError = firstError(cleanup);
					reply.failure(cleanupError == null ? error :
						error + "; partial customer cleanup failed: " + cleanupError);
				});
			}
			else if (allTrue(result))
			{
				reply.success(Integer.valueOf(customerId));
			}
			else
			{
				cleanupPartialCustomer(customerId, successfulTargets(result),
					cleanup -> {
						Collection<ResourceType> created = successfulTargets(result);
						if (firstError(cleanup) != null || !allTargetsTrue(cleanup, created))
						{
							reply.failure("Generated customer registration cleanup failed");
						}
						else
						{
							createGeneratedCustomer(reply);
						}
					});
			}
		});
	}

	private void cleanupPartialCustomer(int customerId, Collection<ResourceType> targets,
		Completion completion)
	{
		if (targets.isEmpty())
		{
			completion.complete(new EnumMap<ResourceType, TcpProtocol.Response>(ResourceType.class));
			return;
		}
		dispatchToMany(targets, "deleteCustomer", new Object[] { Integer.valueOf(customerId) },
			completion);
	}

	private void dispatchToAll(TcpProtocol.Request request, Completion completion)
	{
		dispatchToMany(Arrays.asList(ResourceType.values()), request.method, request.arguments,
			completion);
	}

	private void dispatchToAll(String method, Object[] args, Completion completion)
	{
		dispatchToMany(Arrays.asList(ResourceType.values()), method, args, completion);
	}

	private void dispatchTo(ResourceType type, String method, Object[] args,
		SingleCompletion completion)
	{
		dispatchToMany(Arrays.asList(type), method, args, result ->
			completion.complete(result.get(type)));
	}

	private void dispatchToMany(Collection<ResourceType> targets, String method, Object[] args,
		Completion completion)
	{
		if (targets.isEmpty())
		{
			completion.complete(new EnumMap<ResourceType, TcpProtocol.Response>(ResourceType.class));
			return;
		}
		long id = requestIds.incrementAndGet();
		PendingCall call = new PendingCall(id, targets, completion);
		pending.put(Long.valueOf(id), call);
		TcpProtocol.Request request = new TcpProtocol.Request(id, method, args);
		for (ResourceType type : targets)
		{
			BackendLink backend = backends.get(type);
			try
			{
				backend.send(request);
			}
			catch (IOException e)
			{
				call.accept(type, TcpProtocol.Response.failure(id,
					"Could not send request to " + type.name + ": " + e.getMessage()));
			}
		}
	}

	private void sendClientResponse(ClientReply reply, TcpProtocol.Response response)
	{
		if (response == null)
		{
			reply.failure("ResourceManager did not return a response");
		}
		else if (response.error != null)
		{
			reply.failure(response.error);
		}
		else
		{
			reply.success(response.value);
		}
	}

	private void onBackendResponse(ResourceType type, TcpProtocol.Response response)
	{
		PendingCall call = pending.get(Long.valueOf(response.id));
		if (call != null)
		{
			call.accept(type, response);
		}
	}

	private void onBackendFailure(ResourceType type, String error)
	{
		for (PendingCall call : pending.values())
		{
			if (call.expects(type))
			{
				call.accept(type, TcpProtocol.Response.failure(call.id, error));
			}
		}
	}

	private static ResourceType resourceFor(String method)
	{
		switch (method)
		{
			case "addFlight":
			case "deleteFlight":
			case "queryFlight":
			case "queryFlightPrice":
			case "reserveFlight":
				return ResourceType.FLIGHT;
			case "addCars":
			case "deleteCars":
			case "queryCars":
			case "queryCarsPrice":
			case "reserveCar":
				return ResourceType.CAR;
			case "addRooms":
			case "deleteRooms":
			case "queryRooms":
			case "queryRoomsPrice":
			case "reserveRoom":
				return ResourceType.ROOM;
			default:
				return null;
		}
	}

	private static boolean allTrue(Map<ResourceType, TcpProtocol.Response> responses)
	{
		if (responses.size() != ResourceType.values().length)
		{
			return false;
		}
		for (TcpProtocol.Response response : responses.values())
		{
			if (response == null || response.error != null || !Boolean.TRUE.equals(response.value))
			{
				return false;
			}
		}
		return true;
	}

	private static boolean allTargetsTrue(Map<ResourceType, TcpProtocol.Response> responses,
		Collection<ResourceType> targets)
	{
		if (responses.size() != targets.size())
		{
			return false;
		}
		for (ResourceType type : targets)
		{
			TcpProtocol.Response response = responses.get(type);
			if (response == null || response.error != null || !Boolean.TRUE.equals(response.value))
			{
				return false;
			}
		}
		return true;
	}

	private static String firstError(Map<ResourceType, TcpProtocol.Response> responses)
	{
		for (TcpProtocol.Response response : responses.values())
		{
			if (response == null)
			{
				return "ResourceManager did not return a response";
			}
			if (response.error != null)
			{
				return response.error;
			}
		}
		return null;
	}

	private static Collection<ResourceType> successfulTargets(
		Map<ResourceType, TcpProtocol.Response> responses)
	{
		List<ResourceType> successful = new ArrayList<ResourceType>();
		for (Map.Entry<ResourceType, TcpProtocol.Response> entry : responses.entrySet())
		{
			TcpProtocol.Response response = entry.getValue();
			if (response != null && response.error == null && Boolean.TRUE.equals(response.value))
			{
				successful.add(entry.getKey());
			}
		}
		return successful;
	}

	private static String mergeBills(int customerId,
		Map<ResourceType, TcpProtocol.Response> responses)
	{
		StringBuilder entries = new StringBuilder();
		long total = 0;
		boolean customerExists = false;
		for (ResourceType type : ResourceType.values())
		{
			TcpProtocol.Response response = responses.get(type);
			if (response == null || !(response.value instanceof String))
			{
				continue;
			}
			String managerBill = (String)response.value;
			if (managerBill.isEmpty())
			{
				continue;
			}
			customerExists = true;
			int firstLine = managerBill.indexOf('\n');
			if (firstLine < 0)
			{
				continue;
			}
			String[] lines = managerBill.substring(firstLine + 1).split("\n");
			for (String line : lines)
			{
				if (line.startsWith("Total cost: $"))
				{
					total += Long.parseLong(line.substring("Total cost: $".length()));
				}
				else if (!line.isEmpty())
				{
					entries.append(line).append('\n');
				}
			}
		}
		if (!customerExists)
		{
			return "";
		}
		return "Bill for customer " + customerId + "\n" + entries +
			"Total cost: $" + total + "\n";
	}

	private static void requireCount(TcpProtocol.Request request, int expected)
	{
		if (request.arguments.length != expected)
		{
			throw new IllegalArgumentException("Invalid argument count for " + request.method);
		}
	}

	private static int integer(Object value)
	{
		if (!(value instanceof Integer))
		{
			throw new IllegalArgumentException("Expected integer argument");
		}
		return ((Integer)value).intValue();
	}

	private static String string(Object value)
	{
		if (!(value instanceof String))
		{
			throw new IllegalArgumentException("Expected string argument");
		}
		return (String)value;
	}

	private static boolean bool(Object value)
	{
		if (!(value instanceof Boolean))
		{
			throw new IllegalArgumentException("Expected boolean argument");
		}
		return ((Boolean)value).booleanValue();
	}

	private static Vector<String> stringVector(Object value)
	{
		if (!(value instanceof Vector<?>))
		{
			throw new IllegalArgumentException("Expected a vector of flight numbers");
		}
		Vector<?> vector = (Vector<?>)value;
		Vector<String> result = new Vector<String>(vector.size());
		for (Object item : vector)
		{
			if (!(item instanceof String))
			{
				throw new IllegalArgumentException("Flight numbers must be strings");
			}
			result.add((String)item);
		}
		return result;
	}

	private static String errorMessage(Exception e)
	{
		String message = e.getMessage();
		return e.getClass().getSimpleName() + (message == null ? "" : ": " + message);
	}

	private void acceptBackendResponse(ResourceType type, Socket socket, DataInputStream input)
	{
		try (Socket backendSocket = socket)
		{
			while (running.get())
			{
				onBackendResponse(type, TcpProtocol.readResponse(input));
			}
		}
		catch (IOException e)
		{
			if (running.get())
			{
				onBackendFailure(type, type.name + " ResourceManager connection lost: " + e.getMessage());
			}
		}
	}

	private void closeClient(Socket socket)
	{
		clientSockets.remove(socket);
		try
		{
			socket.close();
		}
		catch (IOException ignored)
		{
		}
	}

	private void closeBackends()
	{
		for (BackendLink backend : backends.values())
		{
			backend.close();
		}
	}

	@Override
	public synchronized void close()
	{
		running.set(false);
		if (listener != null)
		{
			try
			{
				listener.close();
			}
			catch (IOException ignored)
			{
			}
		}
		for (Socket socket : clientSockets)
		{
			closeClient(socket);
		}
		closeBackends();
		clientHandlers.shutdownNow();
	}

	private final class BackendLink
	{
		private final ResourceType type;
		private final Socket socket;
		private final DataInputStream input;
		private final DataOutputStream output;

		BackendLink(ResourceType type, Endpoint endpoint) throws IOException
		{
			this.type = type;
			socket = new Socket();
			socket.connect(new InetSocketAddress(endpoint.host, endpoint.port),
				CONNECT_TIMEOUT_MILLIS);
			output = new DataOutputStream(socket.getOutputStream());
			input = new DataInputStream(socket.getInputStream());
		}

		void startReader()
		{
			Thread reader = new Thread(() -> acceptBackendResponse(type, socket, input),
				"tcp-middleware-" + type.name.toLowerCase() + "-responses");
			reader.setDaemon(true);
			reader.start();
		}

		void send(TcpProtocol.Request request) throws IOException
		{
			synchronized (output)
			{
				TcpProtocol.writeRequest(output, request);
			}
		}

		void close()
		{
			try
			{
				socket.close();
			}
			catch (IOException ignored)
			{
			}
		}
	}

	private final class PendingCall
	{
		private final long id;
		private final Set<ResourceType> targets;
		private final Map<ResourceType, TcpProtocol.Response> responses =
			new EnumMap<ResourceType, TcpProtocol.Response>(ResourceType.class);
		private final Completion completion;
		private boolean completed;

		PendingCall(long id, Collection<ResourceType> targets, Completion completion)
		{
			this.id = id;
			this.targets = EnumSet.noneOf(ResourceType.class);
			this.targets.addAll(targets);
			this.completion = completion;
		}

		boolean expects(ResourceType type)
		{
			return targets.contains(type);
		}

		void accept(ResourceType type, TcpProtocol.Response response)
		{
			Map<ResourceType, TcpProtocol.Response> result = null;
			synchronized (this)
			{
				if (completed || !targets.contains(type) || responses.containsKey(type))
				{
					return;
				}
				responses.put(type, response);
				if (responses.size() == targets.size())
				{
					completed = true;
					result = new EnumMap<ResourceType, TcpProtocol.Response>(responses);
				}
			}
			if (result != null)
			{
				pending.remove(Long.valueOf(id), this);
				completion.complete(result);
			}
		}
	}

	private final class ClientReply
	{
		private final long requestId;
		private final Socket socket;
		private final DataOutputStream output;
		private boolean completed;

		ClientReply(long requestId, Socket socket, DataOutputStream output)
		{
			this.requestId = requestId;
			this.socket = socket;
			this.output = output;
		}

		void success(Object value)
		{
			respond(TcpProtocol.Response.success(requestId, value));
		}

		void failure(String error)
		{
			respond(TcpProtocol.Response.failure(requestId, error));
		}

		private void respond(TcpProtocol.Response response)
		{
			synchronized (this)
			{
				if (completed)
				{
					return;
				}
				completed = true;
			}
			try
			{
				TcpProtocol.writeResponse(output, response);
			}
			catch (IOException e)
			{
				if (running.get())
				{
					System.err.println("Could not reply to client: " + e.getMessage());
				}
			}
			finally
			{
				closeClient(socket);
			}
		}
	}

	private final class BundleExecution
	{
		private final int customerId;
		private final List<BundleStep> steps = new ArrayList<BundleStep>();
		private final List<BundleStep> completedSteps = new ArrayList<BundleStep>();
		private final ClientReply reply;
		private int index;

		BundleExecution(int customerId, Vector<String> flights, String location,
			boolean car, boolean room, ClientReply reply)
		{
			if (flights.isEmpty())
			{
				throw new IllegalArgumentException("A bundle must contain at least one flight");
			}
			this.customerId = customerId;
			this.reply = reply;
			for (String flight : flights)
			{
				Integer.valueOf(flight);
				steps.add(new BundleStep(ResourceType.FLIGHT, "reserveFlight",
					new Object[] { Integer.valueOf(customerId), Integer.valueOf(flight) },
					"flight", flight));
			}
			if (car)
			{
				steps.add(new BundleStep(ResourceType.CAR, "reserveCar",
					new Object[] { Integer.valueOf(customerId), location }, "car", location));
			}
			if (room)
			{
				steps.add(new BundleStep(ResourceType.ROOM, "reserveRoom",
					new Object[] { Integer.valueOf(customerId), location }, "room", location));
			}
		}

		void advance()
		{
			if (index == steps.size())
			{
				reply.success(Boolean.TRUE);
				return;
			}
			BundleStep step = steps.get(index);
			dispatchTo(step.type, step.method, step.arguments, response -> {
				if (response != null && response.error == null && Boolean.TRUE.equals(response.value))
				{
					completedSteps.add(step);
					index++;
					advance();
				}
				else
				{
					String error = response == null ? "ResourceManager did not return a response" :
						response.error;
					rollback(completedSteps.size() - 1, error);
				}
			});
		}

		private void rollback(int rollbackIndex, String cause)
		{
			if (rollbackIndex < 0)
			{
				if (cause == null)
				{
					reply.success(Boolean.FALSE);
				}
				else
				{
					reply.failure("Bundle failed; reservations were rolled back: " + cause);
				}
				return;
			}
			BundleStep step = completedSteps.get(rollbackIndex);
			dispatchTo(step.type, "cancelReservation",
				new Object[] { Integer.valueOf(customerId), step.cancelType, step.identifier },
				response -> {
					if (response == null || response.error != null ||
						!Boolean.TRUE.equals(response.value))
					{
						String rollbackError = response == null ? "missing rollback response" :
							(response.error == null ? "reservation could not be rolled back" :
								response.error);
						reply.failure("Bundle failed (" + cause + "); rollback failed: " + rollbackError);
						return;
					}
					rollback(rollbackIndex - 1, cause);
				});
		}
	}

	private static final class BundleStep
	{
		final ResourceType type;
		final String method;
		final Object[] arguments;
		final String cancelType;
		final String identifier;

		BundleStep(ResourceType type, String method, Object[] arguments,
			String cancelType, String identifier)
		{
			this.type = type;
			this.method = method;
			this.arguments = arguments;
			this.cancelType = cancelType;
			this.identifier = identifier;
		}
	}

	private interface Completion
	{
		void complete(Map<ResourceType, TcpProtocol.Response> responses);
	}

	private interface SingleCompletion
	{
		void complete(TcpProtocol.Response response);
	}

	private static Map<ResourceType, Endpoint> endpoints(String[] args)
	{
		Map<ResourceType, Endpoint> result = new EnumMap<ResourceType, Endpoint>(ResourceType.class);
		for (ResourceType type : ResourceType.values())
		{
			String value = args.length > 0 ? args[type.ordinal()] :
				"localhost:" + type.defaultPort;
			int separator = value.lastIndexOf(':');
			String host = separator < 0 ? value : value.substring(0, separator);
			int port = separator < 0 ? type.defaultPort :
				Integer.parseInt(value.substring(separator + 1));
			result.put(type, new Endpoint(host, port));
		}
		return result;
	}

	public static void main(String[] args) throws Exception
	{
		if (args.length > 4)
		{
			System.err.println("Usage: TCPMiddlewareServer [flight_host[:port] [car_host[:port] " +
				"[room_host[:port] [listen_port]]]]");
			System.exit(2);
		}
		int port = args.length == 4 ? Integer.parseInt(args[3]) : 3042;
		Map<ResourceType, Endpoint> endpoints = endpoints(args);
		TCPMiddlewareServer server = new TCPMiddlewareServer(endpoints, port);
		int boundPort = server.start();
		Runtime.getRuntime().addShutdownHook(new Thread(server::close));
		System.out.println("TCP Middleware listening on port " + boundPort);
		new java.util.concurrent.CountDownLatch(1).await();
	}
}
