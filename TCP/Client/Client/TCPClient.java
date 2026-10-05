package Client;

public final class TCPClient extends Client
{
	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = 3042;

	private final String host;
	private final int port;

	private TCPClient(String host, int port)
	{
		this.host = host;
		this.port = port;
		m_resourceManager = new TcpResourceManagerClient(host, port);
	}

	@Override
	public void connectServer()
	{
		TcpResourceManagerClient client = new TcpResourceManagerClient(host, port);
		try
		{
			client.getName();
		}
		catch (java.rmi.RemoteException e)
		{
			throw new IllegalStateException("Could not connect to TCP Middleware [" +
				host + ":" + port + "]: " + e.getMessage(), e);
		}
		m_resourceManager = client;
		System.out.println("Connected to TCP Middleware [" + host + ":" + port + "]");
	}

	public static void main(String[] args)
	{
		if (args.length > 2)
		{
			System.err.println("Usage: java Client.TCPClient [middleware_host [middleware_port]]");
			System.exit(2);
		}
		try
		{
			String host = args.length > 0 ? args[0] : DEFAULT_HOST;
			int port = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_PORT;
			TCPClient client = new TCPClient(host, port);
			client.connectServer();
			client.start();
		}
		catch (Exception e)
		{
			System.err.println("TCP client could not start: " + e.getMessage());
			System.exit(1);
		}
	}
}
