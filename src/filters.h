
#ifndef _CLEO_FILTERS_INCLUDE_
#define _CLEO_FILTERS_INCLUDE_

enum
{
    F_TASK_TIME = 1 << 1,
    F_TASK_RUN = 1 << 2,
    F_TASK_QUEUE = 1 << 3,
    F_TASK_NP = 1 << 4,
    F_USER_CPU_HOURS = 1 << 5,
    F_QUEUE_CPU_HOURS = 1 << 6
};

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
