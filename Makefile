CC=gcc
CFLAGS=-Wall -Iinclude
SRC=src/logger.c src/utils.c
BIN=bin
DATA=data/files data/logs

all: prepare nm ss client
prepare:
	@mkdir -p $(BIN) $(DATA)
	@echo "[OK] Folders ready."

nm: $(SRC) src/nameserver.c
	$(CC) $(CFLAGS) -o $(BIN)/nm $^

ss: $(SRC) src/storageserver.c
	$(CC) $(CFLAGS) -o $(BIN)/ss $^

client: $(SRC) src/client.c
	$(CC) $(CFLAGS) -o $(BIN)/client $^

clean:
	rm -rf $(BIN)
