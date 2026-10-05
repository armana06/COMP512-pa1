BUILD_DIR := build
SERVER_SOURCES := $(wildcard Server/Server/Interface/*.java) \
	$(wildcard Server/Server/Common/*.java) \
	$(wildcard Server/Server/TCP/*.java)
CLIENT_SOURCES := $(wildcard Client/Client/*.java)

.PHONY: all test clean

all:
	mkdir -p $(BUILD_DIR)
	javac -d $(BUILD_DIR) $(SERVER_SOURCES) $(CLIENT_SOURCES)

test:
	./test.sh

clean:
	rm -rf $(BUILD_DIR)
