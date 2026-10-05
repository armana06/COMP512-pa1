all: compile-client

../Server/RMIInterface.jar: ../Server/Server/Interface/IResourceManager.java
	$(MAKE) -C ../Server RMIInterface.jar

compile-client: ../Server/RMIInterface.jar
	javac -cp ../Server/RMIInterface.jar:. ../Shared/*.java Client/*.java

clean:
	rm -f Client/*.class
