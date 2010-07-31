
#ifndef _ARRAY_INCLUDE_
#define _ARRAY_INCLUDE_

#define MIN_SIZE 16

typedef struct
{
		void **data;
		unsigned int alloc;
		unsigned int len;
} Array;

Array* array_new (unsigned int);

Array* array_append (Array*, void*);

#endif
