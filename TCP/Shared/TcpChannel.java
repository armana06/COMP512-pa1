package Shared;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public final class TcpChannel implements Closeable
{
	private final Socket socket;
	private final ObjectInputStream input;
	private final ObjectOutputStream output;

	public TcpChannel(Socket socket) throws IOException
	{
		ObjectOutputStream newOutput;
		ObjectInputStream newInput;
		try
		{
			newOutput = new ObjectOutputStream(socket.getOutputStream());
			newOutput.flush();
			newInput = new ObjectInputStream(socket.getInputStream());
		}
		catch (IOException e)
		{
			try
			{
				socket.close();
			}
			catch (IOException closeError)
			{
				e.addSuppressed(closeError);
			}
			throw e;
		}
		this.socket = socket;
		output = newOutput;
		input = newInput;
	}

	public void sendRequest(Request request) throws IOException
	{
		writeMessage(request);
	}

	public Request receiveRequest() throws IOException, ClassNotFoundException
	{
		Object message = readMessage();
		if (!(message instanceof Request))
		{
			throw new IOException("Expected a TCP request");
		}
		return (Request)message;
	}

	public void sendResponse(Response response) throws IOException
	{
		writeMessage(response);
	}

	public Response receiveResponse() throws IOException, ClassNotFoundException
	{
		Object message = readMessage();
		if (!(message instanceof Response))
		{
			throw new IOException("Expected a TCP response");
		}
		return (Response)message;
	}

	private void writeMessage(Object message) throws IOException
	{
		output.reset();
		output.writeObject(message);
		output.flush();
	}

	private Object readMessage() throws IOException, ClassNotFoundException
	{
		return input.readObject();
	}

	public void close() throws IOException
	{
		socket.close();
	}
}
