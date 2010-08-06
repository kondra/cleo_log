#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

#include "parse.h"

static int next_token (char **inp,  char bc,  char ec);
static char* get_next_token (char **inp,  char bc,  char ec);
static char* skip_to (char *str, char c);

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

time_t get_time_from_string (char *str)
{
		int i;

		struct tm tp;

    tp.tm_wday  = -1;

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

static int next_token (char **inp,  char bc,  char ec)
{
    char *p;
    char *str = *inp;

    int len;

    while (*str != bc)
        str++;
    p = ++str;
    while (*str != ec)
        str++;

    len = str - p;
    *inp = p;

    return len;
}

static char* get_next_token (char **inp,  char bc,  char ec)
{
    char *p, *res, *str = *inp;
    int len;

    while (*str != bc)
        str++;
    p = ++str;
    while (*str != ec)
        str++;

    len = str - p;
    *inp = str;

    res = (char*) malloc (sizeof (char) * (len + 1));
    strncpy (res,  p,  len);
    res[len] = 0;

    return res;
}

static char* skip_to (char *str, char c)
{
    while (*str != c)
        str++;

    return ++str;
}

LogEntry* parse_string (char *str)
{
		int i, len, q_len;
		char *p, *q_begin;
		char *str2 = str;

    //skip time
    str = skip_to (str, ']');

		//queue name
    q_len = next_token (&str,  ' ',  ' ');
		q_begin = str;
    str += q_len;

    //command name
    len = next_token (&str, ':',  ' ');
    p = str;
    str += len;

		for (i = 0; i < NUM; i++)
				if (strncmp (p, LogEntryString[i], len) == 0)
						break;

		if (i == VIEW)
				return NULL;

    //skip ' '
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
//            entry->view_req.user = get_next_token (&str,  ' ',  ';');
//            entry->view_req.queue = get_next_token (&str,  ' ',  '\n');
//
//						return entry;
				case ADD:
            entry->add_req.user = get_next_token (&str,  ' ',  ';');
            entry->add_req.queue = get_next_token (&str,  ' ',  ';');

            str = skip_to (str, ' ');
            sscanf (str, "%d", &entry->add_req.np);

            entry->add_req.task_with_args = get_next_token (&str,  ' ',  '\n');

						return entry;
				case ADDED:
            sscanf (str, "%d", &entry->added_act.id);

            entry->added_act.parent = get_next_token (&str,  ' ',  ';');

            //skip parent_id
            str++;
            while (*str != ';')
                str++;

            entry->added_act.user = get_next_token (&str,  ' ',  ';');

            str = skip_to (str, ' ');
            sscanf (str, "%d", &entry->added_act.np);

            entry->added_act.task_with_args = get_next_token (&str,  ' ',  '\n');

						return entry;
				case DEL:
            entry->del_req.user = get_next_token (&str,  ' ',  ';');
            entry->del_req.queue = get_next_token (&str,  ' ',  ';');

            str = skip_to (str, ' ');
            sscanf (str, "%d", &entry->del_req.id);

						return entry;
				case PRI:
            entry->pri_req.user = get_next_token (&str, ' ', ';');
            entry->pri_req.queue = get_next_token (&str, ' ', ';');

            str = skip_to (str, ' ');
            sscanf (str, "%d", &entry->pri_req.id);

            str = skip_to (str, ' ');
            sscanf (str, "%d", &entry->pri_req.pri);

						return entry;
				case BLOCK:
            entry->block_req.user = get_next_token (&str, ' ', ';');
            entry->block_req.queue = get_next_token (&str, ' ', ';');

            str = skip_to (str, ' ');
            sscanf (str, "%d", &entry->block_req.id);

            str = skip_to (str, ' ');
            sscanf (str, "%d", &entry->block_req.block);

						return entry;
				case BLOCK_PE:
            entry->block_pe_req.user = get_next_token (&str, ' ', ';');
            entry->block_pe_req.pe = get_next_token (&str, ' ', ';');

            str = skip_to (str, ' ');
            sscanf (str, "%d", &entry->block_pe_req.block);

						return entry;
				case STAT:
            entry->stat_req.user = get_next_token (&str, ' ', ';');
            entry->stat_req.queue = get_next_token (&str, ' ', '\n');

						return entry;
				case RUN:
            sscanf (str, "%d", &entry->run_act.id);

            entry->run_act.user = get_next_token (&str, ' ', ';');

            str = skip_to (str, ' ');
            sscanf (str, "%d", &entry->run_act.np);

            entry->run_act.task_with_args = get_next_token (&str, ' ', '\n');

						return entry;
				case RUN_NODES:
            sscanf (str, "%d", &entry->run_nodes_act.id);

            entry->run_nodes_act.user = get_next_token (&str, ' ',  ';');

            str = skip_to (str, ' ');
            sscanf (str, "%d", &entry->run_nodes_act.np);

            str = skip_to (str, ' ');
            sscanf (str, "%d", &entry->run_nodes_act.np_extra);

            entry->run_nodes_act.nodes_list1 = get_next_token (&str, ' ', ';');
            entry->run_nodes_act.nodes_list2 = get_next_token (&str, ' ', '\n');

						return entry;
				case END_TASK:
            sscanf (str, "%d", &entry->end_task_act.id);

            entry->end_task_act.user = get_next_token (&str, ' ', ';');

            str = skip_to (str, ' ');
            sscanf (str, "%d", &entry->end_task_act.status);

            str = skip_to (str, ' ');
            sscanf (str, "%d", &entry->end_task_act.sig);

            time_t hh, mm, ss;
            str = skip_to (str, ' ');
            sscanf (str, "%ld:%ld:%ld", &hh, &mm, &ss);
            entry->end_task_act.tm = hh * 3600 + mm * 60 + ss;

						return entry;
				case END_TASK_NODES:
            sscanf (str, "%d", &entry->end_task_nodes_act.id);
            entry->end_task_nodes_act.nodes = get_next_token (&str, ' ', '\n');

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
