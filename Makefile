.PHONY: all kvstore start stop clean
all: kvstore
kvstore:
	$(MAKE) -C kvstore
start:
	./start.sh
stop:
	./stop.sh
clean:
	$(MAKE) -C kvstore clean
