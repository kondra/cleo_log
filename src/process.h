
#ifndef _CLEO_PROCESS_INCLUDE_
#define _CLEO_PROCESS_INCLUDE_

typedef struct
{
		int user;
		int id;
		int queue;
		int np;
		int np_extra;
		int sig;

		time_t queue_add_time;
		time_t begin_run_time;
		time_t total_run_time;
} Task;

typedef struct
{
		int task_cnt;
		Task **task;
} User;

typedef struct 
{
		int task_cnt;
		int user_cnt;

		Task **task;
		User **user;
} Queue;

typedef struct
{
		int task_cnt;
		int user_cnt;
		int queue_cnt;

		Task **task;
		User **user;
		Queue **queue;
} Data;

Data* process_log (const char *filename);

#endif
