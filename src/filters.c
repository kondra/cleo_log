#include <string.h>
#include <time.h>

#include "array.h"
#include "hash.h"
#include "process.h"
#include "filters.h"

Array* task_time_filter (Array *task, int b_time, int e_time)
{
    if (task == NULL)
        return NULL;

    Array *res = array_new (0);
    Task *cur_task;

    int i;

    for (i = 0; i < task->len; i++)
    {
        cur_task = (Task*)task->data[i];
        if (cur_task->run_time >= b_time && cur_task->run_time + cur_task->total_run_time <= e_time)
            array_append (res, cur_task);
    }

    return res;
}

Array* task_run_time_filter (Array *task, int min_time, int max_time)
{
    Array *res = array_new (0);
    Task *cur_task;

    int i;

    for (i = 0; i < task->len; i++)
    {
        cur_task = (Task*)task->data[i];
        if (cur_task->total_run_time >= min_time && cur_task->total_run_time <= max_time)
            array_append (res, cur_task);
    }

    return res;
}

Array* task_queue_filter (Array *task, Array *queue)
{
    Array *res = array_new (0);

    Task *cur_task;

    int i, j;
    char *q_name;

    for (i = 0; i < task->len; i++)
    {
        cur_task = (Task*) task->data[i];
        q_name = cur_task->queue;
        for (j = 0; j < queue->len; j++)
            if (strcmp (q_name, queue->data[j]) == 0)
            {
                array_append (res, cur_task);
                break;
            }
    }

    return res;
}

Array* task_np_filter (Array *task, int min_np, int max_np)
{
    Array *res = array_new (0);
    Task *cur_task;

    int i;

    for (i = 0; i < task->len; i++)
    {
        cur_task = (Task*)task->data[i];
        if (cur_task->np >= min_np && cur_task->np <= max_np)
            array_append (res, cur_task);
    }

    return res;
}

Array* user_total_time_filter (Array *users, long long min_time, long long max_time)
{
    Array *res = array_new (0);
    User *cur_user;

    int i;

    for (i = 0; i < users->len; i++)
    {
        cur_user = (User*)users->data[i];
        if (cur_user->total_time >= min_time && cur_user->total_time <= max_time)
            array_append (res, cur_user);
    }

    return res;
}

Array* user_cpu_hours_filter (Array *users, long long min_time, long long max_time)
{
    Array *res = array_new (0);
    User *cur_user;

    int i;

    for (i = 0; i < users->len; i++)
    {
        cur_user = (User*)users->data[i];
        if (cur_user->cpu_hours >= min_time && cur_user->cpu_hours <= max_time)
            array_append (res, cur_user);
    }

    return res;
}

Array* queue_total_time_filter (Array *queues, long long min_time, long long max_time)
{
    Array *res = array_new (0);

    Queue *cur_queue;

    int i;

    for (i = 0; i < queues->len; i++)
    {
        cur_queue = (Queue*) queues->data[i];
        if (cur_queue->total_time >= min_time && cur_queue->total_time <= max_time)
            array_append (res, cur_queue);
    }


    return res;
}

Array* queue_cpu_hours_filter (Array *queues, long long min_time, long long max_time)
{
    Array *res = array_new (0);
    Queue *cur_queue;

    int i;

    for (i = 0; i < queues->len; i++)
    {
        cur_queue = (Queue*)queues->data[i];
        if (cur_queue->cpu_hours >= min_time && cur_queue->cpu_hours <= max_time)
            array_append (res, cur_queue);
    }

    return res;
}
