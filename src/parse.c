#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

#include "parse.h"

static time_t get_time_from_string (char *str);

static void get_view_request (char *str, LogEntry *entry);

static void get_add_request (char *str, LogEntry *entry);

static void get_added_action (char *str, LogEntry *entry);

static void get_del_request (char *str, LogEntry *entry);

static void get_pri_request (char *str, LogEntry *entry);

static void get_block_pe_request (char *str, LogEntry *entry);

static void get_block_request (char *str, LogEntry *entry);

static void get_stat_request (char *str, LogEntry *entry);

static void get_run_action (char *str, LogEntry *entry);

static void get_run_nodes_action (char *str, LogEntry *entry);

static void get_end_task_action (char *str, LogEntry *entry);

static void get_end_task_nodes_action (char *str, LogEntry *entry);

static const char *LogEntryString[] =
{
		"VIEW",
		"ADD",
		"ADDED",
		"DEL",
		"PRI",
		"BLOCK",
		"BLOCK_PE",
		"STAT",
		"RUN",
		"RUN_NODES",
		"END_TASK",
		"END_TASK_NODES"
};

static const char *Mounths[] = 
{
		"Jan",
		"Feb",
		"Mar",
		"Apr",
		"May",
		"Jun",
		"Jul",
		"Aug",
		"Sep",
		"Oct",
		"Nov",
		"Dec"
};

static const char *Days[] =
{
		"Sun",
		"Mon",
		"Tue",
		"Wed",
		"Thu",
		"Fri",
		"Sat"
};

static time_t get_time_from_string (char *str)
{
		int i;

		struct tm tp;

		str++;
		for (i = 0; i < 7; i++)
				if (strncmp (str, Days[i], 3) == 0)
				{
						tp.tm_wday = i;
						break;
				}

		str += 4;
		for (i = 0; i < 12; i++)
				if (strncmp (str, Mounths[i], 3) == 0)
				{
						tp.tm_mon = i;
						break;
				}

		str += 4;
		sscanf (str, "%d %d:%d:%d %d", &tp.tm_mday, &tp.tm_hour, &tp.tm_min, &tp.tm_sec, &tp.tm_year);
		tp.tm_year -= 1900;
		tp.tm_isdst = -1;

		return mktime (&tp);
}

static void get_view_request (char *str, LogEntry *entry)
{
		int len;
		char *p;

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != ';')
				str++;
		len = str - p;
		entry->view_req.user = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->view_req.user, p, len);
		entry->view_req.user[len] = 0;

		p = str = str + 2;
		while (*str != '\n')
				str++;
		len = str - p;
		entry->view_req.queue = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->view_req.queue, p, len);
		entry->view_req.queue[len] = 0;
}

static void get_add_request (char *str, LogEntry *entry)
{
		int len;
		char *p;

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != ';')
				str++;
		len = str - p;
		entry->add_req.user = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->add_req.user, p, len);
		entry->add_req.user[len] = 0;

		p = str = str + 2;
		while (*str != ';')
				str++;
		len = str - p;
		entry->add_req.queue = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->add_req.queue, p, len);
		entry->add_req.queue[len] = 0;

		p = str = str + 2;
		sscanf (str, "%d", &entry->add_req.np);

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != '\n')
				str++;
		len = str - p;
		entry->add_req.task_with_args = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->add_req.task_with_args, p, len);
		entry->add_req.task_with_args[len] = 0;
}

static void get_added_action (char *str, LogEntry *entry)
{
		int len;
		char *p;

		sscanf (str, "%d", &entry->added_act.id);

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != ';')
				str++;
		len = str - p;
		entry->added_act.parent = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->added_act.parent, p, len);
		entry->added_act.parent[len] = 0;

		//skip parent_id
		str++;
		while (*str != ';')
				str++;

		p = str = str + 2;
		while (*str != ';')
				str++;
		len = str - p;
		entry->added_act.user = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->added_act.user, p, len);
		entry->added_act.user[len] = 0;

		p = str = str + 2;
		sscanf (str, "%d", &entry->added_act.np);

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != '\n')
				str++;
		len = str - p;
		entry->added_act.task_with_args = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->added_act.task_with_args, p, len);
		entry->added_act.task_with_args[len] = 0;
}

static void get_del_request (char *str, LogEntry *entry)
{
		int len;
		char *p;

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != ';')
				str++;
		len = str - p;
		entry->del_req.user = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->del_req.user, p, len);
		entry->del_req.user[len] = 0;

		p = str = str + 2;
		while (*str != ';')
				str++;
		len = str - p;
		entry->del_req.queue = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->del_req.queue, p, len);
		entry->del_req.queue[len] = 0;

		while (*str != ' ')
				str++;
		str++;
		sscanf (str, "%d", &entry->del_req.id);
}

static void get_pri_request (char *str, LogEntry *entry)
{
		int len;
		char *p;

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != ';')
				str++;
		len = str - p;
		entry->pri_req.user = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->pri_req.user, p, len);
		entry->pri_req.user[len] = 0;

		p = str = str + 2;
		while (*str != ';')
				str++;
		len = str - p;
		entry->pri_req.queue = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->pri_req.queue, p, len);
		entry->pri_req.queue[len] = 0;

		while (*str != ' ')
				str++;
		str++;
		sscanf (str, "%d", &entry->pri_req.id);

		while (*str != ' ')
				str++;
		str++;
		sscanf (str, "%d", &entry->pri_req.pri);
}

static void get_block_pe_request (char *str, LogEntry *entry)
{
		int len;
		char *p;

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != ';')
				str++;
		len = str - p;
		entry->block_pe_req.user = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->block_pe_req.user, p, len);
		entry->block_pe_req.user[len] = 0;

		p = str = str + 2;
		while (*str != ';')
				str++;
		len = str - p;
		entry->block_pe_req.pe = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->block_pe_req.pe, p, len);
		entry->block_pe_req.pe[len] = 0;

		while (*str != ' ')
				str++;
		str++;
		sscanf (str, "%d", &entry->block_pe_req.block);
}

static void get_block_request (char *str, LogEntry *entry)
{
		int len;
		char *p;

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != ';')
				str++;
		len = str - p;
		entry->block_req.user = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->block_req.user, p, len);
		entry->block_req.user[len] = 0;

		p = str = str + 2;
		while (*str != ';')
				str++;
		len = str - p;
		entry->block_req.queue = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->block_req.queue, p, len);
		entry->block_req.queue[len] = 0;

		while (*str != ' ')
				str++;
		str++;
		sscanf (str, "%d", &entry->block_req.id);

		while (*str != ' ')
				str++;
		str++;
		sscanf (str, "%d", &entry->block_req.block);
}

static void get_stat_request (char *str, LogEntry *entry)
{
		int len;
		char *p;

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != ';')
				str++;
		len = str - p;
		entry->stat_req.user = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->stat_req.user, p, len);
		entry->stat_req.user[len] = 0;

		p = str = str + 2;
		while (*str != '\n')
				str++;
		len = str - p;
		entry->stat_req.queue = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->stat_req.queue, p, len);
		entry->stat_req.queue[len] = 0;
}

static void get_run_action (char *str, LogEntry *entry)
{
		int len;
		char *p;

		sscanf (str, "%d", &entry->run_act.id);

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != ';')
				str++;
		len = str - p;
		entry->run_act.user = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->run_act.user, p, len);
		entry->run_act.user[len] = 0;

		str += 2;
		sscanf (str, "%d", &entry->run_act.np);

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != '\n')
				str++;
		len = str - p;
		entry->run_act.task_with_args = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->run_act.task_with_args, p, len);
		entry->run_act.task_with_args[len] = 0;
}

static void get_run_nodes_action (char *str, LogEntry *entry)
{
		int len;
		char *p;

		sscanf (str, "%d", &entry->run_nodes_act.id);

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != ';')
				str++;
		len = str - p;
		entry->run_nodes_act.user = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->run_nodes_act.user, p, len);
		entry->run_nodes_act.user[len] = 0;

		str += 2;
		sscanf (str, "%d", &entry->run_nodes_act.np);

		while (*str != ' ')
				str++;
		str++;
		sscanf (str, "%d", &entry->run_nodes_act.np_extra);

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != ';')
				str++;
		len = str - p;
		entry->run_nodes_act.nodes_list1 = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->run_nodes_act.nodes_list1, p, len);
		entry->run_nodes_act.nodes_list1[len] = 0;

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != '\n')
				str++;
		len = str - p;
		entry->run_nodes_act.nodes_list2 = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->run_nodes_act.nodes_list2, p, len);
		entry->run_nodes_act.nodes_list2[len] = 0;
}

static void get_end_task_action (char *str, LogEntry *entry)
{
		int len;
		char *p;

		sscanf (str, "%d", &entry->end_task_act.id);

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != ';')
				str++;
		len = str - p;
		entry->end_task_act.user = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->end_task_act.user, p, len);
		entry->end_task_act.user[len] = 0;

		str += 2;
		sscanf (str, "%d", &entry->end_task_act.status);

		while (*str != ' ')
				str++;
		str++;
		sscanf (str, "%d", &entry->end_task_act.sig);

		time_t hh, mm, ss;
		while (*str != ' ')
				str++;
		str++;
		sscanf (str, "%ld:%ld:%ld", &hh, &mm, &ss);
		entry->end_task_act.tm = hh * 3600 + mm * 60 + ss;
}

static void get_end_task_nodes_action (char *str, LogEntry *entry)
{
		int len;
		char *p;

		sscanf (str, "%d", &entry->end_task_nodes_act.id);

		while (*str != ' ')
				str++;
		p = ++str;
		while (*str != '\n')
				str++;
		len = str - p;
		entry->end_task_nodes_act.nodes = (char*) malloc (sizeof (char) * (len + 1));
		strncpy (entry->end_task_nodes_act.nodes, p, len);
		entry->end_task_nodes_act.nodes[len] = 0;
}

LogEntry* parse_string (char *str)
{
		int i, len, q_len;
		char *p, *q_begin;
		char *str2 = str;

		while (*str != ']')
				str++;

		//queue name
		q_begin = str = str + 2;
		while (*str != ' ')
				str++;
		q_len = str - q_begin;

		while (*str != ':')
				str++;
		p = ++str;
		while (*str != ' ')
				str++;
		len = str - p;

		for (i = 0; i < NUM; i++)
				if (strncmp (p, LogEntryString[i], str - p) == 0)
						break;

		if (i == VIEW)
				return NULL;

		str++;

		LogEntry *entry = (LogEntry*) malloc (sizeof (LogEntry));
		entry->tm = get_time_from_string (str2);

		entry->queue = (char*) malloc (sizeof (char) * (q_len + 1));
		strncpy (entry->queue, q_begin, q_len);
		entry->queue[q_len] = 0;

		entry->type = i;

		switch (i)
		{
				case VIEW:
						return NULL;
//						get_view_request (str, entry);
//						return entry;
				case ADD:
						get_add_request (str, entry);
						return entry;
				case ADDED:
						get_added_action (str, entry);
						return entry;
				case DEL:
						get_del_request (str, entry);
						return entry;
				case PRI:
						get_pri_request (str, entry);
						return entry;
				case BLOCK:
						get_block_request (str, entry);
						return entry;
				case BLOCK_PE:
						get_block_pe_request (str, entry);
						return entry;
				case STAT:
						get_stat_request (str, entry);
						return entry;
				case RUN:
						get_run_action (str, entry);
						return entry;
				case RUN_NODES:
						get_run_nodes_action (str, entry);
						return entry;
				case END_TASK:
						get_end_task_action (str, entry);
						return entry;
				case END_TASK_NODES:
						get_end_task_nodes_action (str, entry);
						return entry;
		}

		return NULL;
}

//debug function
void print_log_entry (LogEntry *entry)
{
		if (entry == NULL)
				return;

		switch (entry->type)
		{
				case VIEW:
						printf ("user: %s\nqueue: %s\n\n", entry->view_req.user, entry->view_req.queue);
						return;
				case ADD:
						printf ("user: %s\nqueue: %s\n", entry->add_req.user, entry->add_req.queue);
						printf ("NP: %d\ntask_with_args: %s\n\n", entry->add_req.np, entry->add_req.task_with_args);
						return;
				case ADDED:
						printf ("ADDED:\n");
						printf ("id: %d\nparent: %s\nuser: %s\n", entry->added_act.id, entry->added_act.parent, entry->added_act.user);
						printf ("NP: %d\ntask_with_args: %s\n\n", entry->added_act.np, entry->added_act.task_with_args);
						return;
				case DEL:
						printf ("user: %s\nqueue: %s\nid: %d\n\n", entry->del_req.user, entry->del_req.queue, entry->del_req.id);
						return;
				case PRI:
						printf ("user: %s\nqueue: %s\nid: %d\npri: %d\n\n", entry->pri_req.user, entry->pri_req.queue, entry->pri_req.id, entry->pri_req.pri);
						return;
				case BLOCK:
						printf ("user: %s\nqueue: %s\nid: %d\nblock: %d\n\n", entry->block_req.user, entry->block_req.queue, entry->block_req.id, entry->block_req.block);
						return;
				case BLOCK_PE:
						printf ("user: %s\npe: %s\nblock: %d\n\n", entry->block_pe_req.user, entry->block_pe_req.pe, entry->block_pe_req.block);
						return;
				case STAT:
						printf ("user: %s\nqueue: %s\n\n", entry->stat_req.user, entry->stat_req.queue);
						return;
				case RUN:
						printf ("id: %d\nuser: %s\nNP: %d\ntask_with_args: %s\n\n", entry->run_act.id, entry->run_act.user, entry->run_act.np, entry->run_act.task_with_args);
						return;
				case RUN_NODES:
						printf ("id: %d\nuser: %s\nNP: %d\n", entry->run_nodes_act.id, entry->run_nodes_act.user, entry->run_nodes_act.np);
						printf ("NP_extra: %d\n", entry->run_nodes_act.np_extra);
						printf ("nodes_list1: %s\nnodes_list2: %s\n\n", entry->run_nodes_act.nodes_list1, entry->run_nodes_act.nodes_list2);
						return;
				case END_TASK:
						printf ("id: %d\nuser: %s\nstatus: %d\n", entry->end_task_act.id, entry->end_task_act.user, entry->end_task_act.status);
						printf ("signal: %d\ntime: %ld\n\n", entry->end_task_act.sig, entry->end_task_act.tm);
						return;
				case END_TASK_NODES:
						printf ("id: %d\nnodes: %s\n\n", entry->end_task_nodes_act.id, entry->end_task_nodes_act.nodes);
						return;
				default:
						return;
		}
}

void destroy_log_entry (LogEntry *entry)
{
		if (entry == NULL)
				return;

		switch (entry->type)
		{
				case VIEW:
						free (entry->view_req.user);
						free (entry->view_req.queue);
						break;
				case ADD:
						free (entry->add_req.user);
						free (entry->add_req.queue);
						free (entry->add_req.task_with_args);
						break;
				case ADDED:
						free (entry->added_act.parent);
						free (entry->added_act.user);
						free (entry->added_act.task_with_args);
						break;
				case DEL:
						free (entry->del_req.user);
						free (entry->del_req.queue);
						break;
				case PRI:
						free (entry->pri_req.user);
						free (entry->pri_req.queue);
						break;
				case BLOCK:
						free (entry->block_req.user);
						free (entry->block_req.queue);
						break;
				case BLOCK_PE:
						free (entry->block_pe_req.user);
						free (entry->block_pe_req.pe);
						break;
				case STAT:
						free (entry->stat_req.user);
						free (entry->stat_req.queue);
						break;
				case RUN:
						free (entry->run_act.user);
						free (entry->run_act.task_with_args);
						break;
				case RUN_NODES:
						free (entry->run_nodes_act.user);
						free (entry->run_nodes_act.nodes_list1);
						free (entry->run_nodes_act.nodes_list2);
						break;
				case END_TASK:
						free (entry->end_task_act.user);
						break;
				case END_TASK_NODES:
						free (entry->end_task_nodes_act.nodes);
						break;
				default:
						break;
		}

		free (entry);
}
