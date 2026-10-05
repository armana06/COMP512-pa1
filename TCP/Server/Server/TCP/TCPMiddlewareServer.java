package Server.TCP;

import Shared.Request;
import Shared.Response;
import Shared.TcpChannel;

import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Vector;

public final class TCPMiddlewareServer
{
	private static final int DEFAULT_PORT = 3042;

	public static void main(String[] args)
	{
		if (args.length < 3 || args.length > 7)
		{
			System.err.println("Usage: java Server.TCP.TCPMiddlewareServer "
					+ "<flight_host> <car_host> <room_host> "
					+ "[flight_port [car_port [room_port [middleware_port]]]]");
			System.exit(1);
		}

		EnumMap<ResourceType, Endpoint> endpoints = new EnumMap<ResourceType, Endpoint>(ResourceType.class);
		endpoints.put(ResourceType.FLIGHT, new Endpoint(args[0], portArg(args, 3)));
		endpoints.put(ResourceType.CAR, new Endpoint(args[1], portArg(args, 4)));
		endpoints.put(ResourceType.ROOM, new Endpoint(args[2], portArg(args, 5)));
		int listenPort = portArg(args, 6);

		TCPMiddlewareServer middleware = new TCPMiddlewareServer(endpoints);
		middleware.listen(listenPort);
	}

	private final EnumMap<ResourceType, Endpoint> endpoints;
	private final Object customerLock = new Object();
	private int nextCustomerId = (int)(System.currentTimeMillis() & 0x7fffffff);

	private TCPMiddlewareServer(EnumMap<ResourceType, Endpoint> endpoints)
	{
		this.endpoints = endpoints;
	}

	private void listen(int port)
	{
		try (ServerSocket server = new ServerSocket(port))
		{
			System.out.println("TCP middleware listening on port " + port);
			while (true)
			{
				Socket socket = server.accept();
				new Thread(new ClientHandler(socket), "tcp-middleware-client").start();
			}
		}
		catch (IOException e)
		{
			System.err.println("TCP middleware failed: " + e);
			System.exit(1);
		}
	}

	private Response dispatch(Request request)
	{
		try
		{
			ResourceType type = resourceType(request.command);
			if (type != null)
			{
				return Response.success(call(type, request));
			}

			switch (request.command)
			{
				case "newCustomer":
					if (request.args.length == 0)
					{
						return Response.success(newCustomer());
					}
					return Response.success(newCustomer((Integer)request.args[0]));
				case "deleteCustomer":
					return Response.success(deleteCustomer((Integer)request.args[0]));
				case "queryCustomerInfo":
					return Response.success(queryCustomerInfo((Integer)request.args[0]));
				case "bundle":
					return Response.success(bundle(request));
				case "getName":
					return Response.success("Middleware");
				default:
					throw new IllegalArgumentException("Unknown command: " + request.command);
			}
		}
		catch (RemoteException | RuntimeException e)
		{
			return Response.failure(e.toString());
		}
	}

	private Object call(ResourceType type, Request request) throws RemoteException
	{
		Endpoint endpoint = endpoints.get(type);
		try (TcpChannel channel = new TcpChannel(new Socket(endpoint.host, endpoint.port)))
		{
			channel.sendRequest(request);
			Response response = channel.receiveResponse();
			if (!response.isSuccess())
			{
				throw new RemoteException(type + " resource manager failed: " + response.error);
			}
			return response.result;
		}
		catch (IOException | ClassNotFoundException e)
		{
			throw new RemoteException("Could not communicate with " + type + " resource manager", e);
		}
	}

	private int newCustomer() throws RemoteException
	{
		synchronized (customerLock)
		{
			while (true)
			{
				int customerId = nextCustomerId++;
				if (nextCustomerId <= 0)
				{
					nextCustomerId = 1;
				}

				List<ResourceType> created = new ArrayList<ResourceType>();
				boolean available = true;
				try
				{
					for (ResourceType type : ResourceType.values())
					{
						if (Boolean.TRUE.equals(call(type, Request.of("newCustomer", customerId))))
						{
							created.add(type);
						}
						else
						{
							available = false;
							break;
						}
					}
				}
				catch (RemoteException e)
				{
					rollbackCustomerCreation(customerId, created, e);
					throw e;
				}

				if (available)
				{
					return customerId;
				}
				rollbackCustomerCreation(customerId, created, null);
			}
		}
	}

	private boolean newCustomer(int customerId) throws RemoteException
	{
		synchronized (customerLock)
		{
			List<ResourceType> created = new ArrayList<ResourceType>();
			boolean available = true;
			try
			{
				for (ResourceType type : ResourceType.values())
				{
					if (!Boolean.TRUE.equals(call(type, Request.of("newCustomer", customerId))))
					{
						available = false;
						break;
					}
					created.add(type);
				}
			}
			catch (RemoteException e)
			{
				rollbackCustomerCreation(customerId, created, e);
				throw e;
			}
			if (!available)
			{
				rollbackCustomerCreation(customerId, created, null);
			}
			return available;
		}
	}

	private void rollbackCustomerCreation(int customerId, List<ResourceType> created,
			RemoteException originalFailure) throws RemoteException
	{
		RemoteException rollbackFailure = null;
		for (ResourceType type : created)
		{
			try
			{
				if (!Boolean.TRUE.equals(call(type, Request.of("deleteCustomer", customerId))))
				{
					throw new RemoteException("Customer rollback returned false at " + type);
				}
			}
			catch (RemoteException e)
			{
				if (rollbackFailure == null)
				{
					rollbackFailure = e;
				}
				else
				{
					rollbackFailure.addSuppressed(e);
				}
			}
		}

		if (rollbackFailure != null)
		{
			String cause = originalFailure == null ? "customer ID collision"
					: originalFailure.toString();
			throw new RemoteException("Could not roll back customer creation after " + cause,
					rollbackFailure);
		}
	}

	private boolean deleteCustomer(int customerId) throws RemoteException
	{
		synchronized (customerLock)
		{
			boolean deleted = true;
			for (ResourceType type : ResourceType.values())
			{
				deleted = Boolean.TRUE.equals(call(type,
						Request.of("deleteCustomer", customerId))) && deleted;
			}
			return deleted;
		}
	}

	private String queryCustomerInfo(int customerId) throws RemoteException
	{
		StringBuilder bill = new StringBuilder("Bill for customer ")
				.append(customerId).append('\n');
		long total = 0;
		for (ResourceType type : ResourceType.values())
		{
			String resourceBill = (String)call(type,
					Request.of("queryCustomerInfo", customerId));
			if (resourceBill != null && !resourceBill.isEmpty())
			{
				String[] lines = resourceBill.split("\n");
				for (int i = 1; i < lines.length; ++i)
				{
					if (lines[i].startsWith("Total cost: $"))
					{
						total += Long.parseLong(lines[i].substring("Total cost: $".length()));
					}
					else if (!lines[i].isEmpty())
					{
						bill.append(lines[i]).append('\n');
					}
				}
			}
		}
		bill.append("Total cost: $").append(total).append('\n');
		return bill.toString();
	}

	private boolean bundle(Request request) throws RemoteException
	{
		Object[] args = request.args;
		int customerId = (Integer)args[0];
		Vector<?> requestedFlights = (Vector<?>)args[1];
		String location = (String)args[2];
		boolean car = (Boolean)args[3];
		boolean room = (Boolean)args[4];
		List<Integer> flights = new ArrayList<Integer>();
		for (Object flight : requestedFlights)
		{
			flights.add(Integer.valueOf((String)flight));
		}

		List<ReservationUndo> completed = new ArrayList<ReservationUndo>();
		try
		{
			for (Integer flight : flights)
			{
				if (!reserve(ResourceType.FLIGHT,
						Request.of("reserveFlight", customerId, flight),
						Request.of("cancelReservation", customerId, "flight", flight.toString()),
						completed))
				{
					rollbackBundle(completed);
					return false;
				}
			}
			if (car && !reserve(ResourceType.CAR,
					Request.of("reserveCar", customerId, location),
					Request.of("cancelReservation", customerId, "car", location), completed))
			{
				rollbackBundle(completed);
				return false;
			}
			if (room && !reserve(ResourceType.ROOM,
					Request.of("reserveRoom", customerId, location),
					Request.of("cancelReservation", customerId, "room", location), completed))
			{
				rollbackBundle(completed);
				return false;
			}
			return true;
		}
		catch (RemoteException e)
		{
			try
			{
				rollbackBundle(completed);
			}
			catch (RemoteException rollbackFailure)
			{
				throw new RemoteException("Bundle failed and rollback was incomplete: "
						+ rollbackFailure, e);
			}
			throw e;
		}
	}

	private boolean reserve(ResourceType type, Request reservation, Request undo,
			List<ReservationUndo> completed) throws RemoteException
	{
		if (!Boolean.TRUE.equals(call(type, reservation)))
		{
			return false;
		}
		completed.add(new ReservationUndo(type, undo));
		return true;
	}

	private void rollbackBundle(List<ReservationUndo> completed) throws RemoteException
	{
		RemoteException rollbackFailure = null;
		for (int i = completed.size() - 1; i >= 0; --i)
		{
			ReservationUndo undo = completed.get(i);
			try
			{
				if (!Boolean.TRUE.equals(call(undo.type, undo.request)))
				{
					throw new RemoteException("Could not undo " + undo.request.command
							+ " at " + undo.type);
				}
			}
			catch (RemoteException e)
			{
				if (rollbackFailure == null)
				{
					rollbackFailure = e;
				}
				else
				{
					rollbackFailure.addSuppressed(e);
				}
			}
		}
		completed.clear();
		if (rollbackFailure != null)
		{
			throw new RemoteException("Bundle rollback was incomplete", rollbackFailure);
		}
	}

	private static ResourceType resourceType(String command)
	{
		switch (command)
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

	private static int portArg(String[] args, int index)
	{
		return args.length > index ? Integer.parseInt(args[index]) : DEFAULT_PORT;
	}

	private final class ClientHandler implements Runnable
	{
		private final Socket socket;

		private ClientHandler(Socket socket)
		{
			this.socket = socket;
		}

		public void run()
		{
			try (TcpChannel channel = new TcpChannel(socket))
			{
				while (true)
				{
					Request request = channel.receiveRequest();
					channel.sendResponse(dispatch(request));
				}
			}
			catch (EOFException e)
			{
				// Client closed its persistent connection.
			}
			catch (IOException | ClassNotFoundException e)
			{
				System.err.println("TCP middleware client connection failed: " + e);
			}
		}
	}

	private enum ResourceType
	{
		FLIGHT,
		CAR,
		ROOM
	}

	private static final class Endpoint
	{
		private final String host;
		private final int port;

		private Endpoint(String host, int port)
		{
			this.host = host;
			this.port = port;
		}
	}

	private static final class ReservationUndo
	{
		private final ResourceType type;
		private final Request request;

		private ReservationUndo(ResourceType type, Request request)
		{
			this.type = type;
			this.request = request;
		}
	}
}
