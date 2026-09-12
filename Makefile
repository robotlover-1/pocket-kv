.PHONY: all kvstore test clean

all: kvstore

kvstore:
	$(MAKE) -C kvstore

# 单元测试（不需要启动服务端）
test:
	$(MAKE) -C kvstore test_fullsync_protocol test_vsearch

clean:
	$(MAKE) -C kvstore clean
