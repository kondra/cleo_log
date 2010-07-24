#EXES = $(basename $(wildcard *.c))
CC = gcc
CFLAGS = -Wall -O2

OBJ = main.o parse.o process.o

TARGET = main

#all: 
#	$(MAKE) $(EXES)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) $(OBJ) -o $(TARGET)

process.o: parse.o
main.o: process.o

clean:
	rm -f $(TARGET) *.o 
