#include <stdio.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <float.h>

#include "array.h"
#include "hash.h"
#include "process.h"

int main()
{
		Data *data = data_new (2000, 1000000, 100);

    printf ("processing log file... this may take some time\n");
		process_log (data, "../cleo-short.log");
    printf ("log file processed\n");

    char inp[1000];

    char *str;

    int mask = ~0;
    int new_mask, tmask;
    int filters = ~0;

    long long since = 0;
    long long before = LONG_MAX;

    double mincpuh = 0.;
    double maxcpuh = DBL_MAX;

    int minproc = 0;
    int maxproc = 0;

    long long mintime = 0;
    long long maxtime = LONG_MAX;

    Array *queues = array_new (0);
    Array *users = array_new (0);

    User *cur_user;
    Queue *cur_queue;

    while (1)
    {
        printf (">");
        gets (inp);

        if (strcmp (inp, "help\n") == 0)
        {
            printf ("Basic commands:\n");
            printf ("\tcommand     options                       description\n\n");
            printf ("\tquit                                      quit the program\n\n");
            printf ("\tuser        \"username\"                    print info about user\n");
            printf ("\tuser                                      print info about all users, filtering them if filter parameters are set\n");
            printf ("\tqueue       \"queuename\"                   print info about queue\n");
            printf ("\tqueue                                     print info about all queues, filtering them if filter parameters are set\n");
            printf ("\ttask                                      print info about all tasks, filtering them if filter parameters are set\n");
            printf ("\tsince       dd:mm:yyyy hh:mm              set the beginning time val for tasks filter\n");
            printf ("\tbefore      dd:mm:yyyy hh:mm              set the end time val for tasks filter\n");
            printf ("\tmincpuh     hh (floating point)           set the min cpu hours for users and queues filter\n");
            printf ("\tmaxcpuh     hh (floating point)           set the max cpu hours for users and queues filter\n");
            printf ("\tminproc     n (integer)                   set the min cpu number for tasks filter\n");
            printf ("\tmaxproc     n (integer)                   set the max cpu number for tasks filter\n");
            printf ("\tmintime     hh:mm:ss                      set minimal runtime for tasks filter\n");
            printf ("\tmaxtime     hh:mm:ss                      set maximal runtime for tasks filter\n");
            printf ("\tsetqueue    +|-queue1,...,+|-queueN       set/unset queues for tasks filter\n");
            printf ("\tsetusers    +|-user1,...,+|-userN         set/unset users for tasks filter\n");
            printf ("\tsetdefusers                               filter users, and set them for tasks filter\n");
            printf ("\tunsetusers                                remove all users from list\n");
            printf ("\tdefault                                   set default parameters\n");

            printf ("\nOutput formatting:\n");
            printf ("\t+|-opt\n");
            printf ("\tPrinting optiond:\n");
            printf ("\t\tusername                user name\n");
            printf ("\t\twait                    user\'s total waiting time\n");
            printf ("\t\tkilled                  number of user\'s/queue's killed tasks\n");
//            printf ("\t\tsucc                    number of user\'s/queue's succeded tasks\n");
//            printf ("\t\tunsucc                  number of user\'s/queue's unsucceded tasks\n");
            printf ("\t\tqueuename               queue name\n");
            printf ("\t\tcpuh                    cpu hours\n");
            printf ("\t\tid                      task id\n");
            printf ("\t\tnp                      task cpu numbers\n");
            printf ("\t\tnp_extra                task extra cpu numbers\n");
            printf ("\t\tsignal                  task received signal\n");
            printf ("\t\tadd                     task adding time\n");
            printf ("\t\tbegin                   task begin time\n");
            printf ("\t\ttotal                   task total time\n");
            printf ("\t\ttask                    task count\n");

            continue;
        }
        if (strstr (inp, "queue") == inp)
        {
            if (*(inp + 5) == '\0')
            {
                print_queues (data->aqueue, mask);
            }
            else
            {
                str = inp + 6;
                cur_queue = get_queue (data, str);
                if (cur_queue == NULL)
                {
                    printf ("\tunknown queue\n");
                    continue;
                }
                print_queue (cur_queue, mask);
            }
            continue;
        }
        if (strstr (inp, "user") == inp)
        {
            if (*(inp + 4) == '\0')
            {
                print_users (data->auser, mask);
            }
            else
            {
                str = inp + 5;
                cur_user = get_user (data, str);
                if (cur_user == NULL)
                {
                    printf ("\tunknown user\n");
                    continue;
                }
                print_user (cur_user, mask);
                print_tasks (cur_user->task, mask);
            }
            continue;
        }
        if (*inp == '+' || *inp == '-')
        {
            str = inp;
            new_mask = 0;

            if (mask == (~0))
                mask = 0;

            if (strstr (str, "username") == (str + 1))
                new_mask = P_USER_NAME;
            if (strstr (str, "wait") == (str + 1))
                new_mask = P_WAIT_TIME;
            if (strstr (str, "killed") == (str + 1))
                new_mask = P_KILLED;
            if (strstr (str, "succ") == (str + 1))
                new_mask = P_SUCCEDED;
            if (strstr (str, "unsucc") == (str + 1))
                new_mask = P_UNSUCCEDED;
            if (strstr (str, "queuename") == (str + 1))
                new_mask = P_QUEUE_NAME;
            if (strstr (str, "cpuh") == (str + 1))
                new_mask = P_CPU_HOURS;
            if (strstr (str, "id") == (str + 1))
                new_mask = P_ID;
            if (strstr (str, "np") == (str + 1))
                new_mask = P_NP;
            if (strstr (str, "np_extra") == (str + 1))
                new_mask = P_NP_EXTRA;
            if (strstr (str, "signal") == (str + 1))
                new_mask = P_SIGNAL;
            if (strstr (str, "add") == (str + 1))
                new_mask = P_ADD_TIME;
            if (strstr (str, "begin") == (str + 1))
                new_mask = P_BEGIN_TIME;
            if (strstr (str, "total") == (str + 1))
                new_mask = P_TOTAL_TIME;
            if (strstr (str, "task") == (str + 1))
                new_mask = P_TOTAL_TASK;

            if (new_mask == 0)
            {
                printf ("\tunknown option\n");
                break;
            }

            if (*str == '+')
                mask |= new_mask;
            else
                mask &= (~new_mask);

            continue;
        }
        if (strcmp (inp, "quit") == 0)
        {
            printf ("exiting...\n");
            return 0;
        }
        printf ("\tunknown command\n");
    }

		return 0;
}
