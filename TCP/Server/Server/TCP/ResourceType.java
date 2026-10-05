package Server.TCP;

enum ResourceType
{
	FLIGHT("Flights", 3101),
	CAR("Cars", 3102),
	ROOM("Rooms", 3103);

	final String name;
	final int defaultPort;

	ResourceType(String name, int defaultPort)
	{
		this.name = name;
		this.defaultPort = defaultPort;
	}

	static ResourceType fromName(String name)
	{
		for (ResourceType type : values())
		{
			if (type.name.equalsIgnoreCase(name))
			{
				return type;
			}
		}
		throw new IllegalArgumentException("Resource type must be Flights, Cars, or Rooms");
	}
}
