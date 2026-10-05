package Server.TCP;

import Server.Common.ResourceManager;

import java.rmi.RemoteException;
final class ResourceManagerDispatcher
{
	private ResourceManagerDispatcher()
	{
	}

	static Object invoke(ResourceManager manager, TcpProtocol.Request request) throws RemoteException
	{
		Object[] args = request.arguments;
		switch (request.method)
		{
			case "addFlight":
				requireCount(request, 3);
				return Boolean.valueOf(manager.addFlight(integer(args[0]), integer(args[1]), integer(args[2])));
			case "addCars":
				requireCount(request, 3);
				return Boolean.valueOf(manager.addCars(string(args[0]), integer(args[1]), integer(args[2])));
			case "addRooms":
				requireCount(request, 3);
				return Boolean.valueOf(manager.addRooms(string(args[0]), integer(args[1]), integer(args[2])));
			case "newCustomer":
				if (args.length == 0)
				{
					return Integer.valueOf(manager.newCustomer());
				}
				requireCount(request, 1);
				return Boolean.valueOf(manager.newCustomer(integer(args[0])));
			case "deleteFlight":
				requireCount(request, 1);
				return Boolean.valueOf(manager.deleteFlight(integer(args[0])));
			case "deleteCars":
				requireCount(request, 1);
				return Boolean.valueOf(manager.deleteCars(string(args[0])));
			case "deleteRooms":
				requireCount(request, 1);
				return Boolean.valueOf(manager.deleteRooms(string(args[0])));
			case "deleteCustomer":
				requireCount(request, 1);
				return Boolean.valueOf(manager.deleteCustomer(integer(args[0])));
			case "queryFlight":
				requireCount(request, 1);
				return Integer.valueOf(manager.queryFlight(integer(args[0])));
			case "queryCars":
				requireCount(request, 1);
				return Integer.valueOf(manager.queryCars(string(args[0])));
			case "queryRooms":
				requireCount(request, 1);
				return Integer.valueOf(manager.queryRooms(string(args[0])));
			case "queryCustomerInfo":
				requireCount(request, 1);
				return manager.queryCustomerInfo(integer(args[0]));
			case "queryFlightPrice":
				requireCount(request, 1);
				return Integer.valueOf(manager.queryFlightPrice(integer(args[0])));
			case "queryCarsPrice":
				requireCount(request, 1);
				return Integer.valueOf(manager.queryCarsPrice(string(args[0])));
			case "queryRoomsPrice":
				requireCount(request, 1);
				return Integer.valueOf(manager.queryRoomsPrice(string(args[0])));
			case "reserveFlight":
				requireCount(request, 2);
				return Boolean.valueOf(manager.reserveFlight(integer(args[0]), integer(args[1])));
			case "reserveCar":
				requireCount(request, 2);
				return Boolean.valueOf(manager.reserveCar(integer(args[0]), string(args[1])));
			case "reserveRoom":
				requireCount(request, 2);
				return Boolean.valueOf(manager.reserveRoom(integer(args[0]), string(args[1])));
			case "cancelReservation":
				requireCount(request, 3);
				return Boolean.valueOf(manager.cancelReservation(
					integer(args[0]), string(args[1]), string(args[2])));
			case "getName":
				requireCount(request, 0);
				return manager.getName();
			default:
				throw new IllegalArgumentException("Unsupported ResourceManager method: " + request.method);
		}
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
}
