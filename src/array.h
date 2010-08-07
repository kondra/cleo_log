
#ifndef _ARRAY_INCLUDE_
#define _ARRAY_INCLUDE_

typedef struct
{
    void **data;
    unsigned int alloc;
    unsigned int len;
} Array;

Array* array_new (unsigned int len);
Array* array_append (Array *arr, void *data);

#endif
