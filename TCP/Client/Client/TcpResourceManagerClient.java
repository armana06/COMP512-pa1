package Client;

import Server.Interface.IResourceManager;
import Server.TCP.TcpProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.rmi.RemoteException;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

public final class TcpResourceManagerClient implements IResourceManager
{
	private static final int CONNECT_TIMEOUT_MILLIS = 5000;
	private static final int RESPONSE_TIMEOUT_MILLIS = 30000;

	private final String host;
	private final int port;
	private final AtomicLong requestIds = new AtomicLong();

	public TcpResourceManagerClient(String host, int port)
	{
		if (host == null || host.trim().isEmpty())
		{
			throw new IllegalArgumentException("Middleware host cannot be empty");
		}
		if (port < 1 || port > 65535)
		{
			throw new IllegalArgumentException("Middleware port must be between 1 and 65535");
		}
		this.host = host;
		this.port = port;
	}

	private Object invoke(String method, Object... arguments) throws RemoteException
	{
		long id = requestIds.incrementAndGet();
		try (Socket socket = new Socket())
		{
			socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS);
			socket.setSoTimeout(RESPONSE_TIMEOUT_MILLIS);
			DataOutputStream output = new DataOutputStream(socket.getOutputStream());
			DataInputStream input = new DataInputStream(socket.getInputStream());
			TcpProtocol.writeRequest(output, new TcpProtocol.Request(id, method, arguments));
			TcpProtocol.Response response = TcpProtocol.readResponse(input);
			if (response.id != id)
			{
				throw new IOException("Mismatched response ID: expected " + id + ", received " +
					response.id);
			}
			if (response.error != null)
			{
				throw new RemoteException(response.error);
			}
			return response.value;
		}
		catch (RemoteException e)
		{
			throw e;
		}
		catch (IOException e)
		{
			throw new RemoteException("TCP request " + method + " failed: " + e.getMessage(), e);
		}
	}

	@Override
	public boolean addFlight(int flightNum, int flightSeats, int flightPrice) throws RemoteException
	{
		return bool(invoke("addFlight", flightNum, flightSeats, flightPrice));
	}

	@Override
	public boolean addCars(String location, int numCars, int price) throws RemoteException
	{
		return bool(invoke("addCars", location, numCars, price));
	}

	@Override
	public boolean addRooms(String location, int numRooms, int price) throws RemoteException
	{
		return bool(invoke("addRooms", location, numRooms, price));
	}

	@Override
	public int newCustomer() throws RemoteException
	{
		return integer(invoke("newCustomer"));
	}

	@Override
	public boolean newCustomer(int customerID) throws RemoteException
	{
		return bool(invoke("newCustomer", customerID));
	}

	@Override
	public boolean deleteFlight(int flightNum) throws RemoteException
	{
		return bool(invoke("deleteFlight", flightNum));
	}

	@Override
	public boolean deleteCars(String location) throws RemoteException
	{
		return bool(invoke("deleteCars", location));
	}

	@Override
	public boolean deleteRooms(String location) throws RemoteException
	{
		return bool(invoke("deleteRooms", location));
	}

	@Override
	public boolean deleteCustomer(int customerID) throws RemoteException
	{
		return bool(invoke("deleteCustomer", customerID));
	}

	@Override
	public int queryFlight(int flightNumber) throws RemoteException
	{
		return integer(invoke("queryFlight", flightNumber));
	}

	@Override
	public int queryCars(String location) throws RemoteException
	{
		return integer(invoke("queryCars", location));
	}

	@Override
	public int queryRooms(String location) throws RemoteException
	{
		return integer(invoke("queryRooms", location));
	}

	@Override
	public String queryCustomerInfo(int customerID) throws RemoteException
	{
		return string(invoke("queryCustomerInfo", customerID));
	}

	@Override
	public int queryFlightPrice(int flightNumber) throws RemoteException
	{
		return integer(invoke("queryFlightPrice", flightNumber));
	}

	@Override
	public int queryCarsPrice(String location) throws RemoteException
	{
		return integer(invoke("queryCarsPrice", location));
	}

	@Override
	public int queryRoomsPrice(String location) throws RemoteException
	{
		return integer(invoke("queryRoomsPrice", location));
	}

	@Override
	public boolean reserveFlight(int customerID, int flightNumber) throws RemoteException
	{
		return bool(invoke("reserveFlight", customerID, flightNumber));
	}

	@Override
	public boolean reserveCar(int customerID, String location) throws RemoteException
	{
		return bool(invoke("reserveCar", customerID, location));
	}

	@Override
	public boolean reserveRoom(int customerID, String location) throws RemoteException
	{
		return bool(invoke("reserveRoom", customerID, location));
	}

	@Override
	public boolean bundle(int customerID, Vector<String> flightNumbers, String location,
		boolean car, boolean room) throws RemoteException
	{
		return bool(invoke("bundle", customerID, flightNumbers, location, car, room));
	}

	@Override
	public String getName() throws RemoteException
	{
		return string(invoke("getName"));
	}

	private static boolean bool(Object value) throws RemoteException
	{
		if (!(value instanceof Boolean))
		{
			throw new RemoteException("Invalid response: expected boolean");
		}
		return ((Boolean)value).booleanValue();
	}

	private static int integer(Object value) throws RemoteException
	{
		if (!(value instanceof Integer))
		{
			throw new RemoteException("Invalid response: expected integer");
		}
		return ((Integer)value).intValue();
	}

	private static String string(Object value) throws RemoteException
	{
		if (!(value instanceof String))
		{
			throw new RemoteException("Invalid response: expected string");
		}
		return (String)value;
	}
}
