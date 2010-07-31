#EXES = $(basename $(wildcard *.c))
CC = gcc
CFLAGS = -Wall

OBJ = main.o parse.o process.o hash.o array.o

TARGET = main

#all: 
#	$(MAKE) $(EXES)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) $(OBJ) -o $(TARGET)

process.o: parse.o hash.o array.o
main.o: process.o

clean:
	rm -f $(TARGET) *.o 
