#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "parse.h"
#include "array.h"
#include "hash.h"
#include "process.h"

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

User* user_new (const char *name)
{
		User *user = (User*) malloc (sizeof (User));

		user->task = array_new (0);

		user->name = (char*) malloc (sizeof (char) * (strlen (name) + 1));
		strcpy (user->name, name);

		user->total_time = 0;
		user->cpu_hours = 0;
		user->np = 0;
		user->np_extra = 0;

		return user;
}

Queue* queue_new (const char *name)
{
		Queue *queue = (Queue*) malloc (sizeof (Queue));

		queue->task = array_new (0);
		queue->user = array_new (0);

		queue->name = (char*) malloc (sizeof (char) * (strlen (name) + 1));
		strcpy (queue->name, name);

		queue->total_time = 0;
		queue->cpu_hours = 0;
		queue->np = 0;

		return queue;
}

Task* task_new (const char *user, const char *queue)
{
		Task *task = (Task*) malloc (sizeof (Task));

		task->user = (char*) malloc (sizeof (char) * (strlen (user) + 1));
		strcpy (task->user, user);

		task->queue = (char*) malloc (sizeof (char) * (strlen (queue) + 1));
		strcpy (task->queue, queue);

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
		int cnt = 0;

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
		buf = (char*) malloc (sizeof (char) * 20000);
		key = (char*) malloc (sizeof (char) * 100);

		while (!feof (f))
		{
				fgets (buf, 20000, f);

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
										cur_task = task_new (entry->added_act.parent, entry->added_act.user);
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
								{
//										fprintf (stderr, "user:%s, queue:%s, id:%d\n", entry->end_task_act.user, entry->queue, entry->end_task_act.id);
				//						fprintf (stderr, "FAIL, no task in hash table, process.c, END_TASK\n");
										cnt++;
										break;
//										exit (0);
								}
								if (cur_task->run_time == -1)
								{
										cur_task->deleted = 1;
										break;
								}
								cur_task->sig = entry->end_task_act.sig;
								cur_task->status = entry->end_task_act.status;
								cur_task->total_run_time = entry->tm - cur_task->run_time;
								if ((cur_user = hash_lookup (huser, entry->run_nodes_act.user)) == NULL)
								{
				//						fprintf (stderr, "FAIL, no user in hash table, process.c, END_TASK\n");
										break;
//										exit (0);
								}
								if ((cur_queue = hash_lookup (hqueue, entry->queue)) == NULL)
								{
				//						fprintf (stderr, "FAIL, no queue in hash table, process.c, END_TASK\n");
										break;
//										exit (0);
								}
								cur_user->total_time += cur_task->total_run_time;
								cur_user->np += cur_task->np;
								cur_user->np_extra += cur_task->np_extra;

								cur_queue->total_time += cur_task->total_run_time;
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
//tasks which were runned before log file starts
//		if (cnt)
//				printf ("total %d errors\n", cnt);

		fclose (f);
		free (buf);

		return 0;
}

Array* time_filter (Array *tasks, Predicate pred)
{
		return NULL;
}

void print_queue_tasks (Data *data, const char *queue)
{
		Queue *cur_queue;

		if ((cur_queue = hash_lookup (data->hqueue, queue)) == NULL)
		{
				fprintf (stderr, "no user %s in table\n", queue);
				return;
		}

		int i;
		printf ("\n%d tasks\n\n", cur_queue->task->len);
		for (i = 0; i < cur_queue->task->len; i++)
				print_task (cur_queue->task->data[i]);
}

void print_user_tasks (Data *data, const char *username)
{
		User *cur_user;

		if ((cur_user = hash_lookup (data->huser, username)) == NULL)
		{
				fprintf (stderr, "no user %s in table\n", username);
				return;
		}

		int i;
		printf ("\n%d tasks\n\n", cur_user->task->len);
		for (i = 0; i < cur_user->task->len; i++)
				print_task (cur_user->task->data[i]);
}

void print_task (Task *cur_task)
{
		if (cur_task == NULL)
				return;
		if (cur_task->deleted)
				return;

		printf ("User: %s\n", cur_task->user);
		printf ("Queue: %s\n", cur_task->queue);
		printf ("ID: %d\n", cur_task->id);
		printf ("NP: %d\n", cur_task->np);
		printf ("NP_extra: %d\n", cur_task->np_extra);
		printf ("Signal: %d\n", cur_task->sig);
		printf ("Add time: %ld\n", cur_task->queue_add_time);
		printf ("Begin time: %ld\n", cur_task->run_time);
		printf ("Total time: %lld\n", cur_task->total_run_time);
		printf ("\n");
}

void print_users (Array *auser)
{
		User *cur_user;
		int i;

		printf ("\n%d users\n\n", auser->len);
		for (i = 0; i < auser->len; i++)
		{
				cur_user = auser->data[i];
				printf ("name:%s\ntotal_time:%lld\nNP:%d\nNP_extra:%d\n", cur_user->name, cur_user->total_time, cur_user->np, cur_user->np_extra);
				printf ("\n");
		}
}

void print_queues (Array *aqueue)
{
		Queue *cur_queue;
		int i;

		printf ("\n%d qeues\n\n", aqueue->len);
		for (i = 0; i < aqueue->len; i++)
		{
				cur_queue = aqueue->data[i];
				printf ("%s\n", cur_queue->name);
				printf ("name:%s\ntotal_time:%lld\nNP:%d\n", cur_queue->name, cur_queue->total_time, cur_queue->np);
				printf ("\n");
		}
}

void print_tasks (Array *atask)
{
		int i;
		printf ("\n%d tasks\n\n", atask->len);
		for (i = 0; i < atask->len; i++)
				print_task (atask->data[i]);
}
