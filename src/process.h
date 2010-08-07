
#ifndef _CLEO_PROCESS_INCLUDE_
#define _CLEO_PROCESS_INCLUDE_

enum
{
    P_USER_NAME = 1 << 0,
    P_QUEUE_NAME = 1 << 1,
    P_ID = 1 << 2,
    P_NP = 1 << 3,
    P_NP_EXTRA = 1 << 4,
    P_SIGNAL = 1 << 5,
    P_ADD_TIME = 1 << 6,
    P_BEGIN_TIME = 1 << 7,
    P_TOTAL_TIME = 1 << 8,
    P_KILLED = 1 << 9,
    P_SUCCEDED = 1 << 10,
    P_UNSUCCEDED = 1 << 11,
    P_WAIT_TIME = 1 << 12,
    P_CPU_HOURS = 1 << 13,
};

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
    double cpu_hours;

    time_t queue_add_time;
    time_t run_time;
    time_t total_run_time;
} Task;

typedef struct
{
    char *name;
    long long total_time;
    double cpu_hours;
    long long wait_time;
    int killed;
    int succeded;
    int unsucceded;
    int np;
    int np_extra;

    Array *task; //Task
} User;

typedef struct 
{
    char *name;
    long long total_time;
    double cpu_hours;
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

Data* data_new (int max_user, int max_task, int max_queue);
int process_log (Data *data, const char *filename);
User* get_user (Data *data, const char *username);
Queue* get_queue (Data *data, const char *queue);
void print_tasks (Array *atask, int mask);
void print_users (Array *auser, int mask);
void print_queues (Array *aqueue, int mask);

// Task filters
Array* task_time_filter (Array *task, int b_time, int e_time);
Array* task_run_time_filter (Array *task, int min_time, int max_time);
Array* task_queue_filter (Array *task, Array *queue);
Array* task_np_filter (Array *task, int min_np, int max_np);

// User filters
Array* user_total_time_filter (Array *users, long long min_time, long long max_time);
Array* user_cpu_hours_filter (Array *users, long long min_time, long long max_time);

// Queue filters
Array* queue_total_time_filter (Array *queues, long long min_time, long long max_time);
Array* queue_cpu_hours_filter (Array *queues, long long min_time, long long max_time);

#endif
