#include <time.h>

#include "array.h"
#include "hash.h"
#include "process.h"

int main()
{
		Data *data = data_new (2000, 1000000, 100);
		process_log (data, "../cleo-short.log");
		//print_queue_tasks (data, "regular");
		//print_queues (data->aqueue);

		return 0;
}
