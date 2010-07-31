
#ifndef _CLEO_PROCESS_INCLUDE_
#define _CLEO_PROCESS_INCLUDE_

typedef struct
{
		char* user;
		int id;
		char* queue;
		int np;
		int np_extra;
		int sig;
		int status;
		int deleted;

		time_t queue_add_time;
		time_t run_time;
		long long total_run_time;
} Task;

typedef struct
{
		char *name;
		long long total_time;
		long long cpu_hours;
		int np;
		int np_extra;

		Array *task; //Task
} User;

typedef struct 
{
		char *name;
		long long total_time;
		long long cpu_hours;
		int np;

		Array *task; //Task
		Array *user; //User
} Queue;

typedef struct
{
		HashTable *htask; //Task
		HashTable *huser; //User
		HashTable *hqueue; //Queue

		Array *atask;
		Array *auser;
		Array *aqueue;
} Data;

typedef enum
{
		false = 0,
		true
} boolean;

typedef boolean (*Predicate) (Task*);

Data* data_new (int, int, int);

int process_log (Data*, const char*);

void print_user_tasks (Data*, const char*);

void print_queue_tasks (Data*, const char*);

void print_task (Task*);

void print_users (Array*);

void print_queues (Array*);

void print_tasks (Array*);

#endif
