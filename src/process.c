#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "parse.h"
#include "array.h"
#include "hash.h"
#include "process.h"

#define MAX_BUF_SIZE 20000
#define MAX_KEY_SIZE 100

static User* user_new (const char *name);
static Queue* queue_new (const char *name);
static Task* task_new (const char *user, const char *queue);
static void print_task (Task *cur_task, int mask);
static void print_queue (Queue *cur_queue, int mask);
static void print_user (User *cur_user, int mask);

Data* data_new (int max_user, int max_task, int max_queue)
{
		Data *data = (Data*) malloc (sizeof (Data));

		data->htask = hash_new (max_task);
		data->hqueue = hash_new (max_queue);
		data->huser = hash_new (max_user);

		data->auser = array_new (0);
		data->aqueue = array_new (0);
		data->atask = array_new (0);

		return data;
}

static User* user_new (const char *name)
{
		User *user = (User*) malloc (sizeof (User));

		user->task = array_new (0);

    if (name != NULL)
    {
        user->name = (char*) malloc (sizeof (char) * (strlen (name) + 1));
        strcpy (user->name, name);
    }

		user->total_time = 0;
		user->cpu_hours = 0;
		user->np = 0;
		user->np_extra = 0;
    user->succeded = 0;
    user->unsucceded = 0;
    user->killed = 0;
    user->wait_time = 0;

		return user;
}

static Queue* queue_new (const char *name)
{
		Queue *queue = (Queue*) malloc (sizeof (Queue));

		queue->task = array_new (0);
		queue->user = array_new (0);

    if (name != NULL)
    {
		    queue->name = (char*) malloc (sizeof (char) * (strlen (name) + 1));
		    strcpy (queue->name, name);
    }

		queue->total_time = 0;
		queue->cpu_hours = 0;
		queue->np = 0;

		return queue;
}

static Task* task_new (const char *user, const char *queue)
{
		Task *task = (Task*) malloc (sizeof (Task));

    if (user != NULL)
    {
        task->user = (char*) malloc (sizeof (char) * (strlen (user) + 1));
        strcpy (task->user, user);
    }

    if (queue != NULL)
    {
        task->queue = (char*) malloc (sizeof (char) * (strlen (queue) + 1));
        strcpy (task->queue, queue);
    }

		task->id = 0;
		task->np = 0;
		task->np_extra = 0;
		task->sig = -1;
		task->queue_add_time = -1;
		task->run_time = -1;
		task->total_run_time = -1;
		task->deleted = 0;

		return task;
}

int process_log (Data *data, const char *filename)
{
		FILE *f;
		char *buf, *key;

    if (data == NULL || filename == NULL)
        return -1;

		LogEntry *entry;

		HashTable *htask = data->htask;
		HashTable *huser = data->huser;
		HashTable *hqueue = data->hqueue;

		Array *auser = data->auser;
		Array *aqueue = data->aqueue;
		Array *atask = data->atask;

		User *cur_user;
		Queue *cur_queue;
		Task *cur_task;

		f = fopen (filename, "r");

        if (f == NULL)
        {
                fprintf (stderr, "Unable to open file %s\n", filename);
                return -1;
        }

		buf = (char*) malloc (sizeof (char) * MAX_BUF_SIZE);
		key = (char*) malloc (sizeof (char) * MAX_KEY_SIZE);

		while (!feof (f))
		{
		        fgets (buf, MAX_BUF_SIZE, f);

				entry = parse_string (buf);

				if (entry == NULL)
						continue;

				switch (entry->type)
				{
						case ADDED:
								if ((cur_user = hash_lookup (huser, entry->added_act.user)) == NULL)
								{
										cur_user = user_new (entry->added_act.user);
										hash_insert (huser, entry->added_act.user, cur_user);
										array_append (auser, cur_user);
								}
								if ((cur_queue = hash_lookup (hqueue, entry->added_act.parent)) == NULL)
								{
										cur_queue = queue_new (entry->added_act.parent);
										hash_insert (hqueue, entry->added_act.parent, cur_queue);
										array_append (aqueue, cur_queue);
								}
								sprintf (key, "%s%d", entry->added_act.parent + 2, entry->added_act.id);
								if ((cur_task = hash_lookup (htask, key)) == NULL)
								{
										cur_task = task_new (entry->added_act.user, entry->added_act.parent);
										cur_task->queue_add_time = entry->tm;
										cur_task->id = entry->added_act.id;
										cur_task->np = entry->added_act.np;

										hash_insert (htask, key, cur_task);
										array_append (atask, cur_task);
								}
								array_append (cur_user->task, cur_task);

								array_append (cur_queue->task, cur_task);
								array_append (cur_queue->user, cur_user);
								break;
						case RUN_NODES:
								if ((cur_user = hash_lookup (huser, entry->run_nodes_act.user)) == NULL)
								{
										cur_user = user_new (entry->run_nodes_act.user);
										hash_insert (huser, entry->run_nodes_act.user, cur_user);
										array_append (auser, cur_user);
								}
								if ((cur_queue = hash_lookup (hqueue, entry->queue)) == NULL)
								{
										cur_queue = queue_new (entry->queue);
										hash_insert (hqueue, entry->queue, cur_queue);
										array_append (aqueue, cur_queue);
								}
								sprintf (key, "%s%d", entry->queue + 2, entry->run_nodes_act.id);
								if ((cur_task = hash_lookup (htask, key)) == NULL)
								{
										cur_task = task_new (entry->queue, entry->run_nodes_act.user);
										cur_task->queue_add_time = -1;
										cur_task->id = entry->run_nodes_act.id;
										cur_task->np = entry->run_nodes_act.np;

										hash_insert (htask, key, cur_task);
										array_append (atask, cur_task);
								}
								cur_task->np_extra = entry->run_nodes_act.np_extra;
								cur_task->run_time = entry->tm;
								break;
						case END_TASK:
								sprintf (key, "%s%d", entry->queue + 2, entry->end_task_act.id);
								if ((cur_task = hash_lookup (htask, key)) == NULL)
										break;
                                //it means that task was runned before log file begins
								if (cur_task->run_time == -1)
								{
										cur_task->deleted = 1;
										break;
								}
								cur_task->sig = entry->end_task_act.sig;
								cur_task->status = entry->end_task_act.status;
								cur_task->total_run_time = entry->tm - cur_task->run_time;
								if ((cur_user = hash_lookup (huser, entry->run_nodes_act.user)) == NULL)
										break;
								if ((cur_queue = hash_lookup (hqueue, entry->queue)) == NULL)
										break;

                                if (cur_task->status == 0)
                                        cur_user->succeded++;
                                else
                                        cur_user->unsucceded++;

                                if (cur_task->sig != 0)
                                        cur_user->killed++;

								cur_user->total_time += cur_task->total_run_time;
								cur_user->np += cur_task->np;
								cur_user->np_extra += cur_task->np_extra;
                                cur_user->cpu_hours += cur_task->np * cur_task->total_run_time;
                                cur_user->wait_time += cur_task->run_time - cur_task->queue_add_time;

								cur_queue->total_time += cur_task->total_run_time;
                                cur_queue->cpu_hours += cur_task->np * cur_task->total_run_time;
								cur_queue->np += cur_task->np;
								break;
						case DEL:
								sprintf (key, "%s%d", entry->del_req.queue + 2, entry->del_req.id);
								if ((cur_task = hash_lookup (htask, key)) == NULL)
										break;
								cur_task->deleted = 1;
								break;
						default:
								break;
				}

				destroy_log_entry (entry);
		}

		fclose (f);
		free (buf);
        free (key);

		return 0;
}

User* get_user (Data *data, const char *username)
{
    if (username == NULL || data == NULL)
        return NULL;

		return hash_lookup (data->huser, username);
}

Queue* get_queue (Data *data, const char *queue)
{
        if (queue == NULL || data == NULL)
                return NULL;

		return hash_lookup (data->hqueue, queue);
}

static void print_task (Task *cur_task, int mask)
{
		if (cur_task == NULL)
				return;
		if (cur_task->deleted)
				return;

        if (mask & P_USER_NAME)
        		printf ("User: %s\n", cur_task->user);
        if (mask & P_QUEUE_NAME)
		        printf ("Queue: %s\n", cur_task->queue);
        if (mask & P_ID)
		        printf ("ID: %d\n", cur_task->id);
        if (mask & P_NP)
		        printf ("NP: %d\n", cur_task->np);
        if (mask & P_NP_EXTRA)
		        printf ("NP_extra: %d\n", cur_task->np_extra);
        if (mask & P_SIGNAL)
		        printf ("Signal: %d\n", cur_task->sig);
        if (mask & P_ADD_TIME)
		        printf ("Add time: %ld\n", cur_task->queue_add_time);
        if (mask & P_BEGIN_TIME)
		        printf ("Begin time: %ld\n", cur_task->run_time);
        if (mask & P_TOTAL_TIME)
		        printf ("Total time: %ld\n", cur_task->total_run_time);

		printf ("\n");
}

void print_tasks (Array *atask, int mask)
{
        if (atask == NULL)
                return;

        int i;
		printf ("\n%d tasks\n\n", atask->len);

        for (i = 0; i < atask->len; i++)
                print_task (atask->data[i], mask);
}

static void print_user (User *cur_user, int mask)
{
        if (cur_user == NULL)
                return;

        if (mask & P_USER_NAME)
		        printf ("Name: %s\n", cur_user->name);
        if (mask & P_TOTAL_TIME)
                printf ("Total time: %lld\n", cur_user->total_time);
        if (mask & P_NP)
                printf ("NP: %d\n", cur_user->np);
        if (mask & P_NP_EXTRA)
                printf ("NP_extra: %d\n", cur_user->np_extra);
        if (mask & P_KILLED)
                printf ("killed tasks: %d\n", cur_user->killed);
        if (mask & P_SUCCEDED)
                printf ("succeded tasks: %d\n", cur_user->succeded);
        if (mask & P_UNSUCCEDED)
                printf ("unsucceded tasks: %d\n", cur_user->unsucceded);
        if (mask & P_WAIT_TIME)
                printf ("total wait time: %lld\n", cur_user->wait_time);

        printf ("\n");
}

void print_users (Array *auser, int mask)
{
        if (auser == NULL)
                return;

        int i;
		printf ("\n%d users\n\n", auser->len);
        
        for (i = 0; i < auser->len; i++)
                print_user (auser->data[i], mask);
}

static void print_queue (Queue *cur_queue, int mask)
{
        if (cur_queue == NULL)
                return;

        if (mask & P_QUEUE_NAME)
		        printf ("Name: %s\n", cur_queue->name);
        if (mask & P_TOTAL_TIME)
                printf ("Total time: %lld\n", cur_queue->total_time);
        if (mask & P_NP)
                printf ("NP: %d\n", cur_queue->np);

		printf ("\n");
}

void print_queues (Array *aqueue, int mask)
{
        if (aqueue == NULL)
                return;

        int i;
		printf ("\n%d qeues\n\n", aqueue->len);

        for (i = 0; i < aqueue->len; i++)
                print_queue (aqueue->data[i], mask);
}

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
