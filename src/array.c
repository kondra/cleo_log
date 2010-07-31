#include <stdlib.h>
#include <string.h>

#include "array.h"

static Array* array_resize (Array*);

Array* array_new (unsigned int len)
{
		Array *arr = malloc (sizeof (Array));

		if (len < MIN_SIZE)
				len = MIN_SIZE;

		arr->data = malloc (sizeof (void*) * len);

		arr->alloc = len;
		arr->len = 0;

		return arr;
}

Array* array_append (Array *arr, void *data)
{
		arr = array_resize (arr);

		arr->data[arr->len] = data;
		arr->len ++;

		return arr;
}

static Array* array_resize (Array *arr)
{
		if (arr->alloc == arr->len + 1)
		{
				arr->alloc = arr->alloc * 2;
				arr->data = realloc (arr->data, arr->alloc * sizeof (void*));
		}

		return arr;
}

