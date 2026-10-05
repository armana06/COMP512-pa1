all: compile-server-rmi compile-server-tcp

RMIInterface.jar: Server/Interface/IResourceManager.java
	@echo "Compiling RMI server interface"
	javac Server/Interface/IResourceManager.java
	jar cvf RMIInterface.jar Server/Interface/IResourceManager.class

compile-server-rmi: Server/RMI/*.java Server/Interface/IResourceManager.java Server/Common/*.java
	javac Server/RMI/*.java Server/Interface/IResourceManager.java Server/Common/*.java

compile-server-tcp: Server/TCP/*.java Server/Interface/IResourceManager.java Server/Common/*.java ../Shared/*.java
	javac Server/Interface/IResourceManager.java Server/Common/*.java Server/TCP/*.java ../Shared/*.java

clean:
	rm -f Server/Interface/*.class Server/Common/*.class Server/RMI/*.class Server/TCP/*.class
	rm -f RMIInterface.jar
	rm -f ../Shared/*.class
