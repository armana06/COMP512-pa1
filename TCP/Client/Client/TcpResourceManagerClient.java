package Client;

import Server.Interface.IResourceManager;
import Shared.Request;
import Shared.Response;
import Shared.TcpChannel;

import java.io.IOException;
import java.net.Socket;
import java.rmi.RemoteException;
import java.util.Vector;

public final class TcpResourceManagerClient implements IResourceManager, AutoCloseable
{
	private final String host;
	private final int port;
	private TcpChannel channel;

	public TcpResourceManagerClient(String host, int port) throws IOException
	{
		this.host = host;
		this.port = port;
		channel = new TcpChannel(new Socket(host, port));
	}

	private Object invoke(String command, Object... args) throws RemoteException
	{
		try
		{
			if (channel == null)
			{
				channel = new TcpChannel(new Socket(host, port));
			}
			channel.sendRequest(Request.of(command, args));
			Response response = channel.receiveResponse();
			if (!response.isSuccess())
			{
				throw new RemoteException(response.error);
			}
			return response.result;
		}
		catch (IOException | ClassNotFoundException e)
		{
			closeAfterFailure(e);
			throw new RemoteException("TCP request failed", e);
		}
	}

	private void closeAfterFailure(Exception originalFailure)
	{
		try
		{
			if (channel != null)
			{
				channel.close();
			}
		}
		catch (IOException closeError)
		{
			originalFailure.addSuppressed(closeError);
		}
		finally
		{
			channel = null;
		}
	}

	public boolean addFlight(int flightNum, int flightSeats, int flightPrice) throws RemoteException
	{
		return (Boolean)invoke("addFlight", flightNum, flightSeats, flightPrice);
	}

	public boolean addCars(String location, int numCars, int price) throws RemoteException
	{
		return (Boolean)invoke("addCars", location, numCars, price);
	}

	public boolean addRooms(String location, int numRooms, int price) throws RemoteException
	{
		return (Boolean)invoke("addRooms", location, numRooms, price);
	}

	public int newCustomer() throws RemoteException
	{
		return (Integer)invoke("newCustomer");
	}

	public boolean newCustomer(int customerID) throws RemoteException
	{
		return (Boolean)invoke("newCustomer", customerID);
	}

	public boolean deleteFlight(int flightNum) throws RemoteException
	{
		return (Boolean)invoke("deleteFlight", flightNum);
	}

	public boolean deleteCars(String location) throws RemoteException
	{
		return (Boolean)invoke("deleteCars", location);
	}

	public boolean deleteRooms(String location) throws RemoteException
	{
		return (Boolean)invoke("deleteRooms", location);
	}

	public boolean deleteCustomer(int customerID) throws RemoteException
	{
		return (Boolean)invoke("deleteCustomer", customerID);
	}

	public int queryFlight(int flightNumber) throws RemoteException
	{
		return (Integer)invoke("queryFlight", flightNumber);
	}

	public int queryCars(String location) throws RemoteException
	{
		return (Integer)invoke("queryCars", location);
	}

	public int queryRooms(String location) throws RemoteException
	{
		return (Integer)invoke("queryRooms", location);
	}

	public String queryCustomerInfo(int customerID) throws RemoteException
	{
		return (String)invoke("queryCustomerInfo", customerID);
	}

	public int queryFlightPrice(int flightNumber) throws RemoteException
	{
		return (Integer)invoke("queryFlightPrice", flightNumber);
	}

	public int queryCarsPrice(String location) throws RemoteException
	{
		return (Integer)invoke("queryCarsPrice", location);
	}

	public int queryRoomsPrice(String location) throws RemoteException
	{
		return (Integer)invoke("queryRoomsPrice", location);
	}

	public boolean reserveFlight(int customerID, int flightNumber) throws RemoteException
	{
		return (Boolean)invoke("reserveFlight", customerID, flightNumber);
	}

	public boolean reserveCar(int customerID, String location) throws RemoteException
	{
		return (Boolean)invoke("reserveCar", customerID, location);
	}

	public boolean reserveRoom(int customerID, String location) throws RemoteException
	{
		return (Boolean)invoke("reserveRoom", customerID, location);
	}

	public boolean bundle(int customerID, Vector<String> flightNumbers, String location,
			boolean car, boolean room) throws RemoteException
	{
		return (Boolean)invoke("bundle", customerID, flightNumbers, location, car, room);
	}

	public String getName() throws RemoteException
	{
		return (String)invoke("getName");
	}

	public void close() throws IOException
	{
		if (channel != null)
		{
			channel.close();
			channel = null;
		}
	}
}
