SOURCES=$(shell find lib -name '*.hs' -type f)

format: $(SOURCES)
	stylish-haskell -i $^
