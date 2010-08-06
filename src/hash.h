
#ifndef _HASH_INCLUDE_
#define _HASH_INCLUDE_

typedef struct
{
		void **data;
		char **keys;
		unsigned int size;
		unsigned int mod_mask;
		unsigned int m; // size == 2^m
		unsigned int col;
} HashTable;

HashTable* hash_new (unsigned int size);
void* hash_lookup (HashTable *hash, const char *str);
unsigned int hash_insert (HashTable *hash, const char *str, void *data);

#endif
