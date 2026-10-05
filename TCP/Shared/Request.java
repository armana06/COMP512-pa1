package Shared;

import java.io.Serializable;
import java.util.Objects;

public final class Request implements Serializable
{
	private static final long serialVersionUID = 1L;

	public final String command;
	public final Object[] args;

	private Request(String command, Object[] args)
	{
		this.command = Objects.requireNonNull(command, "command");
		this.args = Objects.requireNonNull(args, "args").clone();
	}

	public static Request of(String command, Object... args)
	{
		return new Request(command, args);
	}
}
