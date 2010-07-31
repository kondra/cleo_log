
#ifndef _CLEO_PARSE_INCLUDE_
#define _CLEO_PARSE_INCLUDE_

typedef enum
{
		VIEW = 0,
		ADD,
		ADDED,
		DEL,
		PRI,
		BLOCK,
		BLOCK_PE,
		STAT,
		RUN,
		RUN_NODES,
		END_TASK,
		END_TASK_NODES,
		NUM
} LogEntryType;

struct ViewRequest
{
		char *user;
		char *queue;
};

struct AddRequest
{
		char *user;
		char *queue;
		int np;
		char *task_with_args;
};

struct AddedAction
{
		int id;
		char *parent;
		int parent_id;
		char *user;
		int np;
		char *task_with_args;
};

struct DelRequest
{
		char *user;
		char *queue;
		int id;
};

struct PriRequest
{
		char *user;
		char *queue;
		int id;
		int pri;
};

struct BlockPeRequest
{
		char *user;
		char *pe;
		int block;
};

struct BlockRequest
{
		char *user;
		char *queue;
		int id;
		int block;
};

struct StatRequest
{
		char *user;
		char *queue;
};

struct RunAction
{
		int id;
		char *user;
		int np;
		char *task_with_args;
};

struct RunNodesAction
{
		int id;
		char *user;
		int np;
		int np_extra;
		char *nodes_list1;
		char *nodes_list2;
};

struct EndTaskAction
{
		int id;
		char *user;
		int status;
		int sig;
		time_t tm;
};

struct EndTaskNodesAction
{
		int id;
		char *nodes;
};

typedef struct
{
		char *queue;
		time_t tm;
		LogEntryType type;
		union {
				struct ViewRequest view_req;
				struct AddRequest add_req;
				struct AddedAction added_act;
				struct DelRequest del_req;
				struct PriRequest pri_req;
				struct BlockPeRequest block_pe_req;
				struct BlockRequest block_req;
				struct StatRequest stat_req;
				struct RunAction run_act;
				struct RunNodesAction run_nodes_act;
				struct EndTaskAction end_task_act;
				struct EndTaskNodesAction end_task_nodes_act;
		};
} LogEntry;

LogEntry* parse_string (char*);

void print_log_entry (LogEntry*);

void destroy_log_entry (LogEntry*);

#endif
