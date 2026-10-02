package Server.Common;

import Server.Interface.*;

import java.util.*;
import java.rmi.RemoteException;
import java.io.*;
import Server.Common.ResItemEnum;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.AlreadyBoundException;
import java.rmi.server.UnicastRemoteObject;
public class Middleware implements IResourceManager{
	HashMap<ResItemEnum, List<IResourceManager>> managers = new HashMap<ResItemEnum, List<IResourceManager>>();	
	//To distribute relatively equally, we naively traverse the list of counters with each operation. Delete operations, which access RM's randomly, will break LRU
	HashMap<ResItemEnum, Integer> lruRM = new HashMap<ResItemEnum,Integer>();
	String name = "Middleware";
	public Middleware() {
		/* TODO
		 * populates managers with managers
		 */
		//initializes counters to -1, since update occurs before each use.
		for(ResItemEnum x : ResItemEnum.values()) {
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
		return -1;
	}

	public boolean newCustomer(int cid) throws RemoteException {
		return false;
	}

	public boolean deleteFlight(int flightNum) throws RemoteException {
		boolean status = false;
		for (IResourceManager x : readManager(ResItemEnum.FLIGHT)) {
			status = status || x.deleteFlight(flightNum);
		}
		return status;
	}

	public boolean deleteCars(String location) throws RemoteException {
		boolean status = false;
		for (IResourceManager x : readManager(ResItemEnum.CAR)) {
			status = status || x.deleteCars(location);
		}
		return status;
	}

	public boolean deleteRooms(String location) throws RemoteException {
		boolean status = false;
		for (IResourceManager x : readManager(ResItemEnum.CAR)) {
			status = status || x.deleteRooms(location);
		}
		return status;
	}

	public boolean deleteCustomer(int customerID) throws RemoteException {
		return false;
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
		return "test";
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
		synchronized (lruRM) {
			updateLRURM(ResItemEnum.FLIGHT);
			return readManager(ResItemEnum.FLIGHT).get(readLRURM(ResItemEnum.FLIGHT)).reserveFlight(customerID, flightNumber);
		}
	} 

	/**
	* Reserve a car at this location.
	*
	* @return Success
	*/
	public boolean reserveCar(int customerID, String location) throws RemoteException{
		if (readManager(ResItemEnum.CAR).isEmpty()) {
			Trace.warn("No car manager available");
			return false;
		}
		synchronized (lruRM) {
			updateLRURM(ResItemEnum.CAR);
			return readManager(ResItemEnum.CAR).get(readLRURM(ResItemEnum.CAR)).reserveCar(customerID, location);
		}
	} 

	/**
	* Reserve a room at this location.
	*
	* @return Success
	*/
	public boolean reserveRoom(int customerID, String location) throws RemoteException{
		if (managers.get(ResItemEnum.ROOM).isEmpty()) {
			Trace.warn("No room manager available");
			return false;
		}
		synchronized (lruRM) {
			updateLRURM(ResItemEnum.ROOM);
			return readManager(ResItemEnum.ROOM).get(readLRURM(ResItemEnum.ROOM)).reserveRoom(customerID, location);
		}	
	} 

	/**
	* Reserve a bundle for the trip.
	*
	* @return Success
	*/
	public boolean bundle(int customerID, Vector<String> flightNumbers, String location, boolean car, boolean room) throws RemoteException{
		return false;
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
		System.out.println("testing");
		Middleware mid = new Middleware();
		Middleware midproxy = (Middleware) UnicastRemoteObject.exportObject(mid, 0);
		//Registry registry = LocateRegistry.getRegistry();
		Registry registry = LocateRegistry.createRegistry(Integer.parseInt(args[0]));
		registry.bind("mid",midproxy);
	}
}

