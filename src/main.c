#include <stdio.h>
#include <string.h>
#include <time.h>

#include "array.h"
#include "hash.h"
#include "process.h"

int main()
{
		Data *data = data_new (2000, 1000000, 100);

    printf ("processing log file... this may take some time\n");
		process_log (data, "../cleo-short.log");
    printf ("log file processed\n");

    char inp[100];
    int mask = ~0;

    while (1)
    {
        printf (">");
        fgets (inp, 100, stdin);

        if (strcmp (inp, "help\n") == 0)
        {
            printf ("Basic commands:\n");
            printf ("\tcommand     options                       description\n\n");
            printf ("\tquit                                      quits the program\n\n");
            printf ("\tuser        \"username\"                    prints info about user\n");
            printf ("\tuser                                      prints info about all users, filtering them if filter parameters are set\n");
            printf ("\tqueue       \"queuename\"                   prints info about queue\n");
            printf ("\tqueue                                     prints info about all queues, filtering them if filter parameters are set\n");
            printf ("\ttask                                      prints info about all tasks, filtering them if filter parameters are set\n");
            printf ("\tsince       dd:mm:yyyy hh:mm              sets the beginning time val for tasks filter\n");
            printf ("\tbefore      dd:mm:yyyy hh:mm              sets the end time val for tasks filter\n");
            printf ("\tmincpuh     hh (floating point)           sets the min cpu hours for users and queues filter\n");
            printf ("\tmaxcpuh     hh (floating point)           sets the max cpu hours for users and queues filter\n");
            printf ("\tminproc     n (integer)                   sets the min cpu number for tasks filter\n");
            printf ("\tmaxproc     n (integer)                   sets the max cpu number for tasks filter\n");
            printf ("\tmintime     hh:mm:ss                      sets minimal runtime for tasks filter\n");
            printf ("\tmaxtime     hh:mm:ss                      sets maximal runtime for tasks filter\n");
            printf ("\tsetqueue    +|-queue1,...,+|-queueN       sets/unsets queues for tasks filter\n");
            printf ("\tsetusers    +|-user1,...,+|-userN         sets/unsets users for tasks filter\n");
            printf ("\tsetdefusers                               filters users, and sets them for tasks filter\n");
            printf ("\tunsetusers                                removes all users from list\n");
            printf ("\tdefault                                   sets default parameters\n");

            printf ("\nOutput formatting:\n");
            printf ("\tprintopt    +|- printing option:\n");
            printf ("\t\tusername                user name\n");
            printf ("\t\twait                    user\'s total waiting time\n");
            printf ("\t\tkilled                  number of user\'s/queue's killed tasks\n");
            printf ("\t\tsucc                    number of user\'s/queue's succeded tasks\n");
            printf ("\t\tunsucc                  number of user\'s/queue's unsucceded tasks\n");
            printf ("\t\tqueuename               queue name\n");
            printf ("\t\tcpuh                    cpu hours\n");
            printf ("\t\tid                      task id\n");
            printf ("\t\tnp                      task cpu numbers\n");
            printf ("\t\tnp_extra                task extra cpu numbers\n");
            printf ("\t\tsignal                  task received signal\n");
            printf ("\t\tadd                     task adding time\n");
            printf ("\t\tbegin                   task begin time\n");
            printf ("\t\ttotal                   task total time\n");

            continue;
        }
        if (strstr (inp, "queue") == inp)
        {
            if (*(inp + 5) == '\n')
            {
                print_queues (data->aqueue, mask);
            }
            continue;
        }
        if (strstr (inp, "user") == inp)
        {
            if (*(inp + 4) == '\n')
            {
                print_users (data->auser, mask);
            }
            continue;
        }

        if (strcmp (inp, "quit\n") == 0)
        {
            printf ("exiting...\n");
            return 0;
        }
    }

		return 0;
}
