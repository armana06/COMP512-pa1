package Shared;

import java.io.Serializable;
import java.util.Objects;

public final class Response implements Serializable
{
	private static final long serialVersionUID = 1L;

	public final Object result;
	public final String error;

	private Response(Object result, String error)
	{
		this.result = result;
		this.error = error;
	}

	public static Response success(Object result)
	{
		return new Response(result, null);
	}

	public static Response failure(String error)
	{
		return new Response(null, Objects.requireNonNull(error, "error"));
	}

	public boolean isSuccess()
	{
		return error == null;
	}
}
