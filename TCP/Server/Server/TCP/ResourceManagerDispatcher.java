package Server.TCP;

import Server.Common.ResourceManager;
import Shared.Request;
import Shared.Response;

import java.rmi.RemoteException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class ResourceManagerDispatcher
{
	private final ResourceManager resourceManager;
	private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock(true);

	public ResourceManagerDispatcher(ResourceManager resourceManager)
	{
		this.resourceManager = resourceManager;
	}

	public Response dispatch(Request request)
	{
		Lock lock = isReadOnly(request.command)
				? stateLock.readLock()
				: stateLock.writeLock();
		lock.lock();
		try
		{
			return Response.success(invoke(request));
		}
		catch (RemoteException | RuntimeException e)
		{
			return Response.failure(e.toString());
		}
		finally
		{
			lock.unlock();
		}
	}

	private Object invoke(Request request) throws RemoteException
	{
		Object[] args = request.args;
		switch (request.command)
		{
			case "addFlight":
				return resourceManager.addFlight((Integer)args[0], (Integer)args[1], (Integer)args[2]);
			case "addCars":
				return resourceManager.addCars((String)args[0], (Integer)args[1], (Integer)args[2]);
			case "addRooms":
				return resourceManager.addRooms((String)args[0], (Integer)args[1], (Integer)args[2]);
			case "newCustomer":
				if (args.length == 0)
				{
					return resourceManager.newCustomer();
				}
				return resourceManager.newCustomer((Integer)args[0]);
			case "deleteFlight":
				return resourceManager.deleteFlight((Integer)args[0]);
			case "deleteCars":
				return resourceManager.deleteCars((String)args[0]);
			case "deleteRooms":
				return resourceManager.deleteRooms((String)args[0]);
			case "deleteCustomer":
				return resourceManager.deleteCustomer((Integer)args[0]);
			case "queryFlight":
				return resourceManager.queryFlight((Integer)args[0]);
			case "queryCars":
				return resourceManager.queryCars((String)args[0]);
			case "queryRooms":
				return resourceManager.queryRooms((String)args[0]);
			case "queryCustomerInfo":
				return resourceManager.queryCustomerInfo((Integer)args[0]);
			case "queryFlightPrice":
				return resourceManager.queryFlightPrice((Integer)args[0]);
			case "queryCarsPrice":
				return resourceManager.queryCarsPrice((String)args[0]);
			case "queryRoomsPrice":
				return resourceManager.queryRoomsPrice((String)args[0]);
			case "reserveFlight":
				return resourceManager.reserveFlight((Integer)args[0], (Integer)args[1]);
			case "reserveCar":
				return resourceManager.reserveCar((Integer)args[0], (String)args[1]);
			case "reserveRoom":
				return resourceManager.reserveRoom((Integer)args[0], (String)args[1]);
			case "cancelReservation":
				return resourceManager.cancelReservation((Integer)args[0],
						(String)args[1], (String)args[2]);
			case "getName":
				return resourceManager.getName();
			default:
				throw new IllegalArgumentException("Unknown command: " + request.command);
		}
	}

	private static boolean isReadOnly(String command)
	{
		switch (command)
		{
			case "queryFlight":
			case "queryCars":
			case "queryRooms":
			case "queryCustomerInfo":
			case "queryFlightPrice":
			case "queryCarsPrice":
			case "queryRoomsPrice":
			case "getName":
				return true;
			default:
				return false;
		}
	}

}
