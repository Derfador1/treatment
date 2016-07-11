CFLAGS+=-std=c11
CFLAGS+=-Wall -Wextra -Wpedantic

TARGETS=sludger chlorinator

all: $(TARGETS)

chlorinator: chlorinator.o structs.o networking.o

sludger: LDLIBS+=-lscrypt
sludger: sludger.o structs.o networking.o

clean:
	$(RM) *.o $(TARGETS)

.phony: all clean
