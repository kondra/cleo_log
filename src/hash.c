#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "hash.h"

static unsigned int hash_func (const char*, unsigned int);

static unsigned int nearest_pow (unsigned int);

static unsigned int fast_log (unsigned int);

static unsigned int set_mask (unsigned int);

HashTable* hash_new (unsigned int size)
{
		HashTable *hash = malloc (sizeof (HashTable));

		hash->size = nearest_pow (size);
		hash->m = fast_log (hash->size);
		hash->mod_mask = set_mask (hash->size);
		hash->col = 0;
		hash->data = calloc (hash->size, sizeof (void*));
		hash->keys = calloc (hash->size, sizeof (char*));

		return hash;
}

void* hash_lookup (HashTable *hash, const char *str)
{
		unsigned int hash_val = hash_func (str, hash->mod_mask);
		unsigned int i = 0, j = hash_val, fine = 0;

		while (hash->keys[j] != NULL && i != hash->size)
		{
				if (strcmp (hash->keys[j], str) == 0)
				{
						fine = 1;
						break;
				}
				i++;
				j = hash_val - i;
		}

		if (fine)
				return (void*) hash->data[j];
		else
				return NULL;
}

unsigned int hash_insert (HashTable *hash, const char *str, void *data)
{
		unsigned int hash_val = hash_func (str, hash->mod_mask);
		unsigned int i = 0, j;

		do
		{
				j = hash_val - i;
				if (hash->keys[j] == NULL)
				{
						hash->data[j] = data;
						hash->keys[j] = (char*) malloc (sizeof (char) * (strlen (str) + 1));
						strcpy (hash->keys[j], str);
						return j;
				}
				hash->col++;
				i++;
		} while (i != hash->size);

		return -1;
}

//PJW hash function
static unsigned int hash_func (const char *str, unsigned int mod_mask)
{
		unsigned int hash_val, i;

		for (hash_val = 0; *str; str++)
		{
				hash_val = (hash_val << 4) + (unsigned char)(*str);

				if ((i = hash_val & 0xf0000000))
						hash_val = (hash_val ^ (i >> 24)) & (0x0fffffff);
		}

		return hash_val & mod_mask;
}

static unsigned int nearest_pow (unsigned int n)
{
		unsigned int m = 1;

		while (m < n && m > 0)
				m <<= 1;

		return m ? m : n;
}

static unsigned int fast_log (unsigned int n)
{
		unsigned int l = 0;

		while (n > 0)
		{
				n >>= 1;
				l++;
		}

		return l;
}

static unsigned int set_mask (unsigned int m)
{
		unsigned int mask = ~m;
		unsigned int i = 0x80000000;

		while ((mask & i) > 0)
		{
				mask &= ~i;
				i >>= 1;
		}

		return mask;
}
