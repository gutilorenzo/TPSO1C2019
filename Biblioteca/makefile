RM=rm -rf
CC=gcc

C_SRCS=$(shell find . -iname "*.c" | tr '\n' ' ')
H_SRCS=$(shell find . -iname "*.h" | tr '\n' ' ')
OBJS=$(C_SRCS:./%.c=build/%.o)

# Clean and compile .so
all: build/liblissandra.so

create-dirs:
	mkdir -p build/mustp_commons

build/liblissandra.so: create-dirs $(OBJS)
	$(CC) -shared -o "build/liblissandra.so" $(OBJS)

build/mustp_commons/%.o: mustp_commons/%.c
	$(CC) -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"

#build/commons/collections/%.o: commons/collections/%.c
#	$(CC) -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"

# Add debug parameters and compile
debug: CC += -DDEBUG -g
debug: all

# Clean build files
clean:
	$(RM) build

install: all
	cp -u build/liblissandra.so /usr/lib
	cp --parents -u $(H_SRCS) /usr/include

uninstall:
	rm -f /usr/lib/liblissandra.so
	rm -rf /usr/include/mustp_commons

.PHONY: all create-dirs clean install uninstall
