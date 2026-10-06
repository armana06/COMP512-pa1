package Server.RMI;

import Server.Interface.*;
import Server.Common.Trace;
import Server.Common.Customer;
import java.util.*;
import java.rmi.RemoteException;
import java.io.*;
import Server.RMI.ResItemEnum;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.AlreadyBoundException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.NotBoundException;
public class Middleware implements IResourceManager{
	HashMap<ResItemEnum, List<IResourceManager>> managers = new HashMap<ResItemEnum, List<IResourceManager>>();	
	//To distribute relatively equally, we naively traverse the list of counters with each operation. Delete operations, which access RM's randomly, will break LRU
	HashMap<ResItemEnum, Integer> lruRM = new HashMap<ResItemEnum,Integer>();
	static int port = 3042;
	static String prefix = "group_42_";
	static String name = "Middleware";
	int cusID = 0;
	public Middleware() {
		//initializes counters to -1, since update occurs before each use.
		//This should be safe, no access will occur at this point.
		for(ResItemEnum x : ResItemEnum.values()) {
			managers.put(x, new ArrayList<IResourceManager>());
			lruRM.put(x, -1);
		}	
		
	}	

	public boolean addFlight(int flightNum, int flightSeats, int flightPrice) throws RemoteException {
		if (readManager(ResItemEnum.FLIGHT).isEmpty()) {
			Trace.warn("No flight manager available");
			return false;
		}
		synchronized (lruRM) {
			updateLRURM(ResItemEnum.FLIGHT);
			return readManager(ResItemEnum.FLIGHT).get(readLRURM(ResItemEnum.FLIGHT)).addFlight(flightNum, flightSeats, flightPrice);
		}
		
	}

	public boolean addCars(String location, int numCars, int price) throws RemoteException {
		if (readManager(ResItemEnum.CAR).isEmpty()) {
			Trace.warn("No car manager available");
			return false;
		}
		synchronized (lruRM) {
			updateLRURM(ResItemEnum.CAR);
			return readManager(ResItemEnum.CAR).get(readLRURM(ResItemEnum.CAR)).addCars(location, numCars, price);
		}
		
		
	}

	public boolean addRooms(String location, int numRooms, int price) throws RemoteException {
		if (managers.get(ResItemEnum.ROOM).isEmpty()) {
			Trace.warn("No room manager available");
			return false;
		}
		synchronized (lruRM) {
			updateLRURM(ResItemEnum.ROOM);
			return readManager(ResItemEnum.ROOM).get(readLRURM(ResItemEnum.ROOM)).addRooms(location, numRooms, price);
		}	
		
	}

	public int newCustomer() throws RemoteException {
		//TODO: Figure out how to handle what to do if one of them throws an error. It might be fine through redundacy? If the customer is never added then it just has no clue for any other related call? But if a flight is added to it it might delete it accidentally?
		for(ResItemEnum x : ResItemEnum.values()) {
			for(IResourceManager i : readManager(x)) {
				//since all RM's have the same customers, this should only have to loop for the first object, ensuring that every RM has the same cid.
				while(!i.newCustomer(cusID)) {
					++cusID;
				}
			}
		}
		return cusID++;
	}

	public boolean newCustomer(int cid) throws RemoteException {
		boolean status = true;
		for(ResItemEnum x : ResItemEnum.values()) {
			for(IResourceManager i : readManager(x)) {
				status = i.newCustomer(cid) && status; 
			}
		}
		return status;
	}

	public boolean deleteFlight(int flightNum) throws RemoteException {
		boolean status = true;
		for (IResourceManager x : readManager(ResItemEnum.FLIGHT)) {
			status = status && x.deleteFlight(flightNum);
		}
		return status;
	}

	public boolean deleteCars(String location) throws RemoteException {
		boolean status = true;
		for (IResourceManager x : readManager(ResItemEnum.CAR)) {
			status =  x.deleteCars(location) || status;
		}
		return status;
	}

	public boolean deleteRooms(String location) throws RemoteException {
		boolean status = true;
		for (IResourceManager x : readManager(ResItemEnum.ROOM)) {
			status =  x.deleteRooms(location) || status;
		}
		return status;
	}

	public boolean deleteCustomer(int customerID) throws RemoteException {
		boolean status = true;
		for(ResItemEnum x : ResItemEnum.values()) {
			for(IResourceManager i : readManager(x)) {
				status = i.deleteCustomer(customerID) && status;
			}
		}
		return status;
	}

	public int queryFlight(int flightNumber) throws RemoteException {
		for(IResourceManager x : readManager(ResItemEnum.FLIGHT)) {
			if (x.queryFlight(flightNumber) > 0) {
				return x.queryFlight(flightNumber);
			}
		}
		return 0;
	}

	public int queryCars(String location) throws RemoteException {
		for(IResourceManager x : readManager(ResItemEnum.CAR)) {
			if (x.queryCars(location) > 0) {
				return x.queryCars(location);
			}
		}
		return 0;
	}

	public int queryRooms(String location) throws RemoteException {
		for(IResourceManager x : readManager(ResItemEnum.ROOM)) {
			if (x.queryRooms(location) > 0) {
				return x.queryRooms(location);
			}
		}
		return 0;
	}

	public String queryCustomerInfo(int customerID) throws RemoteException {
		String info = "Bill for customer " + customerID + "\n"; 
		for (ResItemEnum x : ResItemEnum.values()) {
			for (IResourceManager i : readManager(x)) {
				info += i.queryCustomerInfo(customerID).split("\n", 2)[1];
			}
		}
		return info;
	}

	public int queryFlightPrice(int flightNumber) throws RemoteException {
		for(IResourceManager x : readManager(ResItemEnum.FLIGHT)) {
			if (x.queryFlightPrice(flightNumber) > 0) {
				return x.queryFlightPrice(flightNumber);
			}
		}
		return 0;
	}

	/**
	* Query the status of a car location.
	*
	* @return Price of car
	*/
	public int queryCarsPrice(String location) throws RemoteException{
		for(IResourceManager x : readManager(ResItemEnum.CAR)) {
			if (x.queryCarsPrice(location) > 0) {
				return x.queryCarsPrice(location);
			}
		}
		return 0;
	} 

	/**
	* Query the status of a room location.
	*
	* @return Price of a room
	*/
	public int queryRoomsPrice(String location) throws RemoteException{
		for(IResourceManager x : readManager(ResItemEnum.ROOM)) {
			if (x.queryRoomsPrice(location) > 0) {
				return x.queryRoomsPrice(location);
			}
		}
		return 0;
	} 

	/**
	* Reserve a seat on this flight.
	*
	* @return Success
	*/
	public boolean reserveFlight(int customerID, int flightNumber) throws RemoteException{
		if (readManager(ResItemEnum.FLIGHT).isEmpty()) {
			Trace.warn("No flight manager available");
			return false;
		}
		boolean status = true;
		for(IResourceManager x : readManager(ResItemEnum.FLIGHT)) {
			status = x.reserveFlight(customerID, flightNumber) && status;
		}
		return status;
	} 

	/**
	* Reserve a car at this location.
	*
	* @return Success
	*/
	public boolean reserveCar(int customerID, String location) throws RemoteException{
		if (readManager(ResItemEnum.CAR).isEmpty()) {
			Trace.warn("No Car manager available");
			return false;
		}
		boolean status = true;
		for(IResourceManager x : readManager(ResItemEnum.CAR)) {
			status = x.reserveCar(customerID, location) && status;
			System.out.println("Reserving Car?");
		}
		return status;
	} 

	/**
	* Reserve a room at this location.
	*
	* @return Success
	*/
	public boolean reserveRoom(int customerID, String location) throws RemoteException{
		if (readManager(ResItemEnum.ROOM).isEmpty()) {
			Trace.warn("No Room manager available");
			return false;
		}
		boolean status = true;
		for(IResourceManager x : readManager(ResItemEnum.ROOM)) {
			status = x.reserveRoom(customerID, location) && status;
		}
		return status;
	} 

	/**
	* Reserve a bundle for the trip.
	*
	* @return Success
	*/
	public boolean bundle(int customerID, Vector<String> flightNumbers, String location, boolean car, boolean room) throws RemoteException{
		boolean status = true;
		for(String s : flightNumbers) {
			status = reserveFlight(customerID, Integer.parseInt(s)) && status;
			if(!status) {
				deleteCustomer(customerID);
				newCustomer(customerID);
				return status;
			}
		}
		if(car) {
			Trace.warn("entered car");
			status = reserveCar(customerID, location) && status;
			if(!status) {
				deleteCustomer(customerID);
				newCustomer(customerID);
				return status;
			}
		}
		if(room) {
			Trace.warn("entered room");
			status = reserveRoom(customerID, location) && status;
			if(!status) {
				deleteCustomer(customerID);
				newCustomer(customerID);
				return status;
			}

		}
		return status;
	} 

	/**
	* Convenience for probing the resource manager.
	*
	* @return Name
	*/
	public String getName() throws RemoteException{
		return name;
	}

	//safe read for manager
	private List<IResourceManager> readManager(ResItemEnum type) {
		synchronized(managers) {
			return managers.get(type);	
		}
	}
	
	//safe add
	private void addManager(ResItemEnum type, IResourceManager manager)
	{
		synchronized(managers) {
			managers.get(type).add(manager);
		}
	}

	//safe remove 
	private void removeManager(ResItemEnum type, IResourceManager manager)
	{
		synchronized(managers) {
			managers.get(type).remove(manager);
		}
	}

	//safe read for lruRM, updates counter.
	private Integer readLRURM(ResItemEnum type) {
		synchronized(lruRM) {
			return	lruRM.get(type);
		}
	}

	//Update, must be used with a synchronise on lruRM if used with an update to ensure that LRU is maintained.
	private void updateLRURM(ResItemEnum type) {
		lruRM.put(type, (lruRM.get(type) + 1) % (managers.get(type).size()));
	}

	public static void main(String[] args) throws RemoteException, AlreadyBoundException {
		Middleware mid = new Middleware();
		try {
			for(ResItemEnum x : ResItemEnum.values()) {
				for(String s : parseArgs(args).get(x)) {
					Trace.warn(x.toString() + " " + s);
					mid.connectServer(s, 3042, "Resources", x);
				}
			}
			mid.makeServer(mid);
		} 
		catch (Exception e) {    
			System.err.println((char)27 + "[31;1mClient exception: " + (char)27 + "[0mUncaught exception");
			e.printStackTrace();
			System.exit(1);
		}

	}
	private void makeServer(Middleware mid) throws RemoteException, AlreadyBoundException {
		try {
			IResourceManager midproxy = (IResourceManager) UnicastRemoteObject.exportObject(mid, 0);
			Registry l_registry;
			try {
				l_registry = LocateRegistry.createRegistry(port);
			} catch (RemoteException e) {
				l_registry = LocateRegistry.getRegistry(port);
			}
			final Registry registry = l_registry;
			registry.rebind(mid.prefix + mid.name, midproxy);

			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					try {
						registry.unbind(mid.prefix + mid.name);
						System.out.println("'" + mid.name + "' resource manager unbound");
					}
					catch(Exception e) {
						System.err.println((char)27 + "[31;1mServer exception: " + (char)27 + "[0mUncaught exception");
						e.printStackTrace();
					}
				}
			});                                       
			System.out.println("'" + mid.name + "' resource manager server ready and bound to '" + mid.prefix + mid.name + "'");
		}
		catch (Exception e) {
			System.err.println((char)27 + "[31;1mServer exception: " + (char)27 + "[0mUncaught exception");
			e.printStackTrace();
			System.exit(1);
		}
	}
	private void connectServer(String server, int port, String name, ResItemEnum type)
	{
		try {
			boolean first = true;
			while (true) {
				try {
					Registry registry = LocateRegistry.getRegistry(server, port);
					readManager(type).add((IResourceManager)registry.lookup(prefix + name));
					System.out.println("Connected to '" + name + "' server [" + server + ":" + port + "/" + prefix + name + "]");
					break;
				}
				catch (NotBoundException|RemoteException e) {
					if (first) {
						System.out.println("Waiting for '" + name + "' server [" + server + ":" + port + "/" + prefix + name + "]");
						first = false;
					}
				}
				Thread.sleep(500);
			}
		}
		catch (Exception e) {
			System.err.println((char)27 + "[31;1mServer exception: " + (char)27 + "[0mUncaught exception");
			e.printStackTrace();
			System.exit(1);
		}
	}
	private static HashMap<ResItemEnum, List<String>> parseArgs(String[] args) {
		HashMap<ResItemEnum, List<String>> hosts = new HashMap<ResItemEnum,  List<String>>();
		int i = 0;
		for(ResItemEnum x : ResItemEnum.values()) {
			ArrayList<String> hostList = new ArrayList<String>();
			for(int j = 0; j <  args[i].length(); ++j) {
				if(hostList.isEmpty() || args[i].charAt(j) == ',') {
					if(hostList.isEmpty()) {--j;}
					hostList.add("");
				}	
				else {
					hostList.set(hostList.size() - 1, hostList.get(hostList.size() - 1) + args[i].charAt(j));
				}
			}	
			++i;
			hosts.put(x, hostList);
		}
		return hosts;
	}

}

