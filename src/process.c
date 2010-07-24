#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "parse.h"
#include "process.h"

Data* process_log (const char *filename)
{
		FILE *f;
		char *buf;

		LogEntry *entry;
//		Data *data;

		f = fopen (filename, "r");
		buf = (char*) malloc (sizeof (char) * 20000);

		while (!feof (f))
		{
				fgets (buf, 20000, f);

				entry = parse_string (buf);
				destroy_log_entry (entry);
		}

		fclose (f);
		free (buf);

		return NULL;
}
