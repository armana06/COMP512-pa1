package Server.TCP;

import Server.Common.ResourceManager;
import Shared.Request;
import Shared.Response;
import Shared.TcpChannel;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public final class TCPResourceManagerServer
{
	private static final int DEFAULT_PORT = 3042;

	public static void main(String[] args)
	{
		if (args.length > 2)
		{
			System.err.println("Usage: java Server.TCP.TCPResourceManagerServer [name [port]]");
			System.exit(1);
		}

		String name = args.length > 0 ? args[0] : "Resources";
		int port = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_PORT;
		ResourceManagerDispatcher dispatcher =
				new ResourceManagerDispatcher(new ResourceManager(name));

		try (ServerSocket server = new ServerSocket(port))
		{
			System.out.println("'" + name + "' TCP resource manager listening on port " + port);
			while (true)
			{
				Socket socket = server.accept();
				new Thread(new RequestHandler(socket, dispatcher), "tcp-rm-client").start();
			}
		}
		catch (IOException e)
		{
			System.err.println("TCP resource manager failed: " + e);
			System.exit(1);
		}
	}

	private static final class RequestHandler implements Runnable
	{
		private final Socket socket;
		private final ResourceManagerDispatcher dispatcher;

		private RequestHandler(Socket socket, ResourceManagerDispatcher dispatcher)
		{
			this.socket = socket;
			this.dispatcher = dispatcher;
		}

		public void run()
		{
			try (TcpChannel channel = new TcpChannel(socket))
			{
				Request request = channel.receiveRequest();
				Response response = dispatcher.dispatch(request);
				channel.sendResponse(response);
			}
			catch (IOException | ClassNotFoundException e)
			{
				System.err.println("TCP resource manager connection failed: " + e);
			}
		}
	}
}
