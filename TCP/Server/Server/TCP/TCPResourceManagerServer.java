package Server.TCP;

import Server.Common.ResourceManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TCPResourceManagerServer implements AutoCloseable
{
	private final ResourceManager manager;
	private final int requestedPort;
	private final ExecutorService connections = Executors.newCachedThreadPool();
	private final ExecutorService requests = Executors.newFixedThreadPool(
		Math.max(4, Math.min(16, Runtime.getRuntime().availableProcessors())));
	private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
	private final AtomicBoolean running = new AtomicBoolean();
	private ServerSocket listener;

	public TCPResourceManagerServer(ResourceManager manager, int port)
	{
		this.manager = manager;
		this.requestedPort = port;
	}

	public synchronized int start() throws IOException
	{
		if (running.get())
		{
			return listener.getLocalPort();
		}
		listener = new ServerSocket(requestedPort);
		running.set(true);
		Thread acceptor = new Thread(this::acceptConnections, "tcp-rm-accept");
		acceptor.setDaemon(true);
		acceptor.start();
		return listener.getLocalPort();
	}

	private void acceptConnections()
	{
		while (running.get())
		{
			try
			{
				Socket socket = listener.accept();
				sockets.add(socket);
				connections.execute(() -> serveConnection(socket));
			}
			catch (IOException e)
			{
				if (running.get())
				{
					System.err.println("ResourceManager accept failed: " + e.getMessage());
				}
			}
		}
	}

	private void serveConnection(Socket socket)
	{
		try (Socket client = socket;
			 DataInputStream input = new DataInputStream(client.getInputStream());
			 DataOutputStream output = new DataOutputStream(client.getOutputStream()))
		{
			while (running.get() && !client.isClosed())
			{
				TcpProtocol.Request request;
				try
				{
					request = TcpProtocol.readRequest(input);
				}
				catch (EOFException e)
				{
					break;
				}
				requests.execute(() -> executeRequest(request, output));
			}
		}
		catch (IOException e)
		{
			if (running.get())
			{
				System.err.println("ResourceManager connection failed: " + e.getMessage());
			}
		}
		finally
		{
			sockets.remove(socket);
		}
	}

	private void executeRequest(TcpProtocol.Request request, DataOutputStream output)
	{
		TcpProtocol.Response response;
		try
		{
			response = TcpProtocol.Response.success(
				request.id, ResourceManagerDispatcher.invoke(manager, request));
		}
		catch (Exception e)
		{
			response = TcpProtocol.Response.failure(request.id, errorMessage(e));
		}

		synchronized (output)
		{
			try
			{
				TcpProtocol.writeResponse(output, response);
			}
			catch (IOException e)
			{
				if (running.get())
				{
					System.err.println("ResourceManager response failed: " + e.getMessage());
				}
			}
		}
	}

	private static String errorMessage(Exception e)
	{
		String message = e.getMessage();
		return e.getClass().getSimpleName() + (message == null ? "" : ": " + message);
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
		for (Socket socket : sockets)
		{
			try
			{
				socket.close();
			}
			catch (IOException ignored)
			{
			}
		}
		connections.shutdownNow();
		requests.shutdownNow();
	}

	public static void main(String[] args) throws Exception
	{
		if (args.length < 1 || args.length > 2)
		{
			System.err.println("Usage: TCPResourceManagerServer <Flights|Cars|Rooms> [port]");
			System.exit(2);
		}
		ResourceType type = ResourceType.fromName(args[0]);
		int port = args.length == 2 ? Integer.parseInt(args[1]) : type.defaultPort;
		TCPResourceManagerServer server =
			new TCPResourceManagerServer(new ResourceManager(type.name), port);
		int boundPort = server.start();
		Runtime.getRuntime().addShutdownHook(new Thread(server::close));
		System.out.println(type.name + " TCP ResourceManager listening on port " + boundPort);
		new java.util.concurrent.CountDownLatch(1).await();
	}
}
