#EXES = $(basename $(wildcard *.c))
CC = gcc
CFLAGS = -Wall

OBJ = main.o parse.o process.o hash.o array.o filters.o

TARGET = main

#all: 
#	$(MAKE) $(EXES)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) $(OBJ) -o $(TARGET)

filters.o: process.o array.o hash.o
process.o: parse.o hash.o array.o
main.o: process.o array.o hash.o

clean:
	rm -f $(TARGET) *.o 
