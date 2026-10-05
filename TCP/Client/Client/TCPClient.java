package Client;

import java.io.IOException;

public final class TCPClient extends Client
{
	private static final int DEFAULT_PORT = 3042;

	private final String host;
	private final int port;
	private TcpResourceManagerClient connection;

	private TCPClient(String host, int port)
	{
		this.host = host;
		this.port = port;
	}

	public static void main(String[] args)
	{
		if (args.length > 2)
		{
			System.err.println("Usage: java Client.TCPClient [middleware_host [port]]");
			System.exit(1);
		}

		String host = args.length > 0 ? args[0] : "localhost";
		int port = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_PORT;

		try
		{
			TCPClient client = new TCPClient(host, port);
			client.connectServer();
			client.start();
		}
		catch (Exception e)
		{
			System.err.println("TCP client error: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void connectServer()
	{
		try
		{
			if (connection != null)
			{
				connection.close();
			}
			connection = new TcpResourceManagerClient(host, port);
			m_resourceManager = connection;
			System.out.println("Connected to middleware [" + host + ":" + port + "]");
		}
		catch (IOException e)
		{
			throw new IllegalStateException("Could not connect to middleware at " + host + ":" + port, e);
		}
	}
}
