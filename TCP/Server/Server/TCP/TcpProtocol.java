package Server.TCP;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Vector;

public final class TcpProtocol
{
	private static final int MAX_ARGUMENTS = 64;
	private static final int MAX_VECTOR_ITEMS = 10000;

	private static final int INTEGER = 1;
	private static final int BOOLEAN = 2;
	private static final int STRING = 3;
	private static final int STRING_VECTOR = 4;
	private static final int NULL = 0;

	private TcpProtocol()
	{
	}

	public static final class Request
	{
		public final long id;
		public final String method;
		public final Object[] arguments;

		public Request(long id, String method, Object[] arguments)
		{
			this.id = id;
			this.method = method;
			this.arguments = arguments.clone();
		}
	}

	public static final class Response
	{
		public final long id;
		public final Object value;
		public final String error;

		public Response(long id, Object value, String error)
		{
			this.id = id;
			this.value = value;
			this.error = error;
		}

		public static Response success(long id, Object value)
		{
			return new Response(id, value, null);
		}

		public static Response failure(long id, String error)
		{
			return new Response(id, null, error);
		}
	}

	public static void writeRequest(DataOutputStream output, Request request) throws IOException
	{
		if (request.arguments.length > MAX_ARGUMENTS)
		{
			throw new IOException("Too many request arguments");
		}
		output.writeLong(request.id);
		output.writeUTF(request.method);
		output.writeInt(request.arguments.length);
		for (Object argument : request.arguments)
		{
			writeValue(output, argument);
		}
		output.flush();
	}

	public static Request readRequest(DataInputStream input) throws IOException
	{
		long id = input.readLong();
		String method = input.readUTF();
		int count = input.readInt();
		if (count < 0 || count > MAX_ARGUMENTS)
		{
			throw new IOException("Invalid request argument count: " + count);
		}
		Object[] arguments = new Object[count];
		for (int i = 0; i < count; i++)
		{
			arguments[i] = readValue(input);
		}
		return new Request(id, method, arguments);
	}

	public static void writeResponse(DataOutputStream output, Response response) throws IOException
	{
		output.writeLong(response.id);
		output.writeBoolean(response.error != null);
		if (response.error != null)
		{
			output.writeUTF(response.error);
		}
		else
		{
			writeValue(output, response.value);
		}
		output.flush();
	}

	public static Response readResponse(DataInputStream input) throws IOException
	{
		long id = input.readLong();
		if (input.readBoolean())
		{
			return Response.failure(id, input.readUTF());
		}
		return Response.success(id, readValue(input));
	}

	private static void writeValue(DataOutputStream output, Object value) throws IOException
	{
		if (value == null)
		{
			output.writeByte(NULL);
		}
		else if (value instanceof Integer)
		{
			output.writeByte(INTEGER);
			output.writeInt(((Integer)value).intValue());
		}
		else if (value instanceof Boolean)
		{
			output.writeByte(BOOLEAN);
			output.writeBoolean(((Boolean)value).booleanValue());
		}
		else if (value instanceof String)
		{
			output.writeByte(STRING);
			output.writeUTF((String)value);
		}
		else if (value instanceof Vector<?>)
		{
			Vector<?> vector = (Vector<?>)value;
			if (vector.size() > MAX_VECTOR_ITEMS)
			{
				throw new IOException("Too many vector items");
			}
			output.writeByte(STRING_VECTOR);
			output.writeInt(vector.size());
			for (Object item : vector)
			{
				if (!(item instanceof String))
				{
					throw new IOException("Only string vectors are supported");
				}
				output.writeUTF((String)item);
			}
		}
		else
		{
			throw new IOException("Unsupported TCP value type: " + value.getClass().getName());
		}
	}

	private static Object readValue(DataInputStream input) throws IOException
	{
		int type = input.readUnsignedByte();
		switch (type)
		{
			case NULL:
				return null;
			case INTEGER:
				return Integer.valueOf(input.readInt());
			case BOOLEAN:
				return Boolean.valueOf(input.readBoolean());
			case STRING:
				return input.readUTF();
			case STRING_VECTOR:
				int count = input.readInt();
				if (count < 0 || count > MAX_VECTOR_ITEMS)
				{
					throw new IOException("Invalid vector size: " + count);
				}
				Vector<String> values = new Vector<String>(count);
				for (int i = 0; i < count; i++)
				{
					values.add(input.readUTF());
				}
				return values;
			default:
				throw new IOException("Unknown TCP value type: " + type);
		}
	}
}
