# Makefile for permission-test tools

CC=gcc
CFLAGS=-Wall -Wextra -O2
LDFLAGS=

TARGETS=permission-test jit-code-modification-test

all: $(TARGETS)

permission-test: permission-test.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

jit-code-modification-test: jit-code-modification-test.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

# Quick targets
jit-test: jit-code-modification-test

clean:
	rm -f $(TARGETS)

.PHONY: all clean jit-test