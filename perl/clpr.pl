#!/usr/bin/perl 

use strict;
use warnings;

use String::Format  qw(stringf);
use Getopt::Std     qw(getopts);
use Storable        qw(store retrieve);
use POSIX           qw(mktime ctime);

#===  FUNCTION  ================================================================
#         NAME:  parse_log_record
#   PARAMETERS:  string from log
#      RETURNS:  hash with record parameters
#===============================================================================
sub parse_log_record {
    my $str = shift;

    my %res;
    my %mounths = ("Jan", 0, "Feb", 1, "Mar", 2, "Apr", 3, "May", 4, "Jun", 5, "Jul", 6, "Aug", 7, "Sep", 8, "Oct", 9, "Nov", 10, "Dec", 11);

    if ($str =~ /\[(\w+)\s(\w+)\s+(\d+)\s(\d+):(\d+):(\d+)\s(\d+)\]/) {
        $res{TIME} = mktime ($6, $5, $4, $3, $mounths{$2}, $7 - 1900, 0, 0, -1);
    }

    my $req_queue = $1 if ($str =~ /\]\s(\w+)\s/);

    my @words = split /;\s|;;\s/, $str;

    my $msg_type = $1 if ($str =~ /:([A-Za-z_]+[0]*)\s/);
    $res{MSG_TYPE} = $msg_type;

    if ($msg_type eq "ADDED") {
        $res{ID} = $1 if ($words[0] =~ /\s(\w+)$/);
        $res{QUEUE} = $words[1];
        if (/;;/) {
            $res{USER} = $words[2];
            $res{NP} = $words[3];
        } else {
            $res{USER} = $words[3];
            $res{NP} = $words[4];
        }
    } elsif ($msg_type eq "DEL") {
        $res{USER} = $1 if ($words[0] =~ /\s(\w+)$/);
        $res{QUEUE} = $words[1];
        chomp ($res{ID} = $words[2]);
    } elsif ($msg_type eq "RUN_NODES") {
        $res{ID} = $1 if ($words[0] =~ /\s(\w+)$/);
        $res{USER} = $words[1];
        $res{NP} = $words[2];
        $res{NP_EXTRA} = $words[3];
        $res{QUEUE} = $req_queue;
    } elsif ($msg_type eq "END_TASK0") {
        $res{ID} = $1 if ($words[0] =~ /\s(\w+)$/);
        $res{QUEUE} = $req_queue;
    } elsif ($msg_type eq "END_TASK") {
        $res{ID} = $1 if ($words[0] =~ /\s(\w+)$/);
        $res{USER} = $words[1];
        $res{STATUS} = $words[2];
        $res{SIGNAL} = $words[3];
        $res{QUEUE} = $req_queue;
    }
    %res;
}

#===  FUNCTION  ================================================================
#         NAME:  print_task
#   PARAMETERS:  task hash pointer, format string
#  DESCRIPTION:  prints task info with sprintf like formatting
#===============================================================================
sub print_task {
    my $cur_task = shift;
    my $format = shift;
    
    my %args = (
        i => $cur_task->{ID},
        q => $cur_task->{QUEUE},
        u => $cur_task->{USER},
        s => sub { $cur_task->{SIGNAL} if exists $cur_task->{SIGNAL} },
        t => sub { $cur_task->{STATUS} if exists $cur_task->{STATUS} },
        n => sub { $cur_task->{NP} },
        x => sub { $cur_task->{NP_EXTRA} if exists $cur_task->{NP_EXTRA} },
        c => sub { sprintf "%.2lf", $cur_task->{CPU_HOURS} / 3600 if exists $cur_task->{CPU_HOURS} },
        r => sub {
            if (exists $cur_task->{RUN_TIME}) {
                my $hh = $cur_task->{RUN_TIME} / 3600;
                my $mm = ($cur_task->{RUN_TIME} % 3600) / 60;
                my $ss = ($cur_task->{RUN_TIME} % 3600) % 60;
                return sprintf "%02.0lf:%02.0lf:%02.0lf", $hh, $mm, $ss;
            }
        },
        a => sub { chomp (my $t = ctime ($cur_task->{ADD_TIME})) if exists $cur_task->{ADD_TIME}; $t },
        b => sub { chomp (my $t = ctime ($cur_task->{BEGIN_TIME})) if exists $cur_task->{BEGIN_TIME}; $t },
        e => sub { chomp (my $t = ctime ($cur_task->{ADD_TIME})) if exists $cur_task->{ADD_TIME}; $t },
    );

    print stringf $format, %args;
}

#===  FUNCTION  ================================================================
#         NAME:  print_stat
#   PARAMETERS:  stat, user or queue hash pointer, format string
#  DESCRIPTION:  prints stat with sprintf like formatting
#===============================================================================
sub print_stat {
    my $stat = shift;
    my $format = shift;

    my %args = (
        h => sub { sprintf "%.2lf", $stat->{CPU_HOURS} / 3600 if exists $stat->{CPU_HOURS} },
        w => sub {
            if (exists $stat->{WAIT_TIME}) {
                my $hh = $stat->{WAIT_TIME} / 3600;
                my $mm = ($stat->{WAIT_TIME} % 3600) / 60;
                my $ss = ($stat->{WAIT_TIME} % 3600) % 60;
                sprintf "%02.0lf:%02.0lf:%02.0lf", $hh, $mm, $ss;
            }
        },
        a => sub { my $cnt = $stat->{TASKS} },
        s => sub { $stat->{SUCCEDED} if exists $stat->{SUCCEDED} },
        u => sub { $stat->{UNSUCCEDED} if exists $stat->{UNSUCCEDED} },
        k => sub { $stat->{KILLED} if exists $stat->{KILLED} },
        d => sub { $stat->{DELETED} if exists $stat->{DELETED} },
        D => sub { $stat->{DELETED_BEFORE_RUN} if exists $stat->{DELETED} },
    );

    print stringf $format, %args;
}

#===  FUNCTION  ================================================================
#         NAME:  collect_stat
#      PURPOSE:  collect statistic from array of tasks
#   PARAMETERS:  task array pointer
#      RETURNS:  stat hash pointer
#===============================================================================
sub collect_stat {
    my $tasks = shift;
    my %stat;
    my $i = 0;

    $stat{DELETED} = $stat{DELETED_BEFORE_RUN} = 0;

    foreach my $cur_task (@$tasks) {
        if (defined $cur_task->{USED}) {
            $stat{WAIT_TIME} += $cur_task->{BEGIN_TIME} - $cur_task->{ADDED_TIME} if exists $cur_task->{BEGIN_TIME};
            $stat{CPU_HOURS} += $cur_task->{CPU_HOURS} if exists $cur_task->{CPU_HOURS};
            if (exists $cur_task->{SIGNAL} && exists $cur_task->{SIGNAL}) {
                $stat{SUCCEDED}++ if ($cur_task->{SIGNAL} == 0 && $cur_task->{STATUS} == 0);
                $stat{UNSUCCEDED}++ if ($cur_task->{SIGNAL} || $cur_task->{STATUS});
                $stat{KILLED}++ if ($cur_task->{SIGNAL});
            }
            $stat{DELETED}++ if (exists $cur_task->{DEL});
            $stat{DELETED_BEFORE_RUN}++ if (exists $cur_task->{DEL_BEFORE_RUN});
            $i++;
        }
    }
    $stat{TASKS} = $i;
    \%stat;
}

#===  FUNCTION  ================================================================
#         NAME:  process_log
#      PURPOSE:  main processing function
#   PARAMETERS:  link to file descriptor
#      RETURNS:  hash with tasks, users and queues
#===============================================================================
sub process_log {
    local *LOG = shift;
    my $result = shift;

    my ($all_tasks, $all_queues, $all_users);
    my (@new_tasks, %new_queues, %new_users);

    if (exists $result->{TASKS}) {
        $all_tasks = $result->{TASKS};
    } else {
        $result->{TASKS} = $all_tasks = \@new_tasks;
    }

    if (exists $result->{QUEUES}) {
        $all_queues = $result->{QUEUES}
    } else {
        $result->{QUEUES} = $all_queues = \%new_queues;
    }

    if (exists $result->{USERS}) {
        $all_users = $result->{USERS}
    } else {
        $result->{USERS} = $all_users = \%new_users;
    }

    my $prev_pos = tell LOG;
    my $i = 0;
    while (<LOG>) {
        $i++;
#        last if ($i > 50000);
        $result->{LAST_POS} = $prev_pos;
        $result->{LAST} = $_;

        my %cur_task;
        my %data = parse_log_record $_;
        if ($data{MSG_TYPE} eq "ADDED") {
            $cur_task{ID} = $data{ID};
            $cur_task{USER} = $data{USER};
            $cur_task{QUEUE} = $data{QUEUE};
            $cur_task{NP} = $1 if ($data{NP} =~ /(\d+)-?/); # what is "1-as" "1-"
            $cur_task{ADDED_TIME} = $data{TIME};
            $cur_task{USED} = undef;

            push @$all_tasks, \%cur_task;
            
            if (exists $all_queues->{$cur_task{QUEUE}}) {
                push @{$all_queues->{$cur_task{QUEUE}}->{TASKS}}, \%cur_task;
                $all_queues->{$cur_task{QUEUE}}->{TASKS_HASH}->{$cur_task{ID}} = \%cur_task;
            } else {
                my %new_queue;
                my @queue_tasks;
                my %queue_tasks_hash;
                push @queue_tasks, \%cur_task; 
                $queue_tasks_hash{$cur_task{ID}} = \%cur_task;
                $new_queue{TASKS} = \@queue_tasks;
                $new_queue{TASKS_HASH} = \%queue_tasks_hash;
                $all_queues->{$cur_task{QUEUE}} = \%new_queue;
            }

            if (exists $all_users->{$cur_task{USER}}) {
                push @{$all_users->{$cur_task{USER}}->{TASKS}}, \%cur_task;
            } else {
                my %new_user;
                my @user_tasks;
                push @user_tasks, \%cur_task;
                $new_user{TASKS} = \@user_tasks;
                $all_users->{$cur_task{USER}} = \%new_user;
            }
        } elsif ($data{MSG_TYPE} eq "RUN_NODES") {
            if (exists $all_queues->{$data{QUEUE}}->{TASKS_HASH}->{$data{ID}}) {
                my $cur_task = $all_queues->{$data{QUEUE}}->{TASKS_HASH}->{$data{ID}};
                $cur_task->{BEGIN_TIME} = $data{TIME};
                $cur_task->{NP_EXTRA} = $data{NP_EXTRA};

                my $cur_user = $all_users->{$cur_task->{USER}};
                $cur_user->{WAIT_TIME} += $cur_task->{BEGIN_TIME} - $cur_task->{ADDED_TIME};
            } else {
                warn "RUN_NODES: task with id '$data{ID}' from queue '$data{QUEUE}' doesn't exist in database\n";
            }
        } elsif ($data{MSG_TYPE} eq "END_TASK") {
            if (exists $all_queues->{$data{QUEUE}}->{TASKS_HASH}->{$data{ID}}) {
                my $cur_task = $all_queues->{$data{QUEUE}}->{TASKS_HASH}->{$data{ID}};
                $cur_task->{END_TIME} = $data{TIME};
                $cur_task->{SIGNAL} = $data{SIGNAL};
                $cur_task->{STATUS} = $data{STATUS};

                my $cur_user = $all_users->{$cur_task->{USER}};
                my $cur_queue = $all_queues->{$cur_task->{QUEUE}};

                if (defined $cur_task->{BEGIN_TIME}) {
                    $cur_task->{RUN_TIME} = $cur_task->{END_TIME} - $cur_task->{BEGIN_TIME};

                    my $ch = ($cur_task->{END_TIME} - $cur_task->{BEGIN_TIME}) * $cur_task->{NP};
                    $cur_task->{CPU_HOURS} = $ch;

                    $cur_user->{CPU_HOURS} += $ch;
                    $cur_queue->{CPU_HOURS} += $ch;

                    $cur_queue->{SUCCEDED}++, $cur_user->{SUCCEDED}++ if ($cur_task->{SIGNAL} == 0 && $cur_task->{SIGNAL} == 0);
                    $cur_queue->{UNSUCCEDED}++, $cur_user->{UNSUCCEDED}++ if ($cur_task->{SIGNAL} || $cur_task->{SIGNAL});
                    $cur_queue->{KILLED}++, $cur_user->{KILLED}++ if ($cur_task->{SIGNAL});
                } else {
                    $cur_task->{DEL_BEFORE_RUN} = 1;
                    $cur_user->{DELETED_BEFORE_RUN}++;
                    $cur_queue->{DELETED_BEFORE_RUN}++;
                }
            } else {
                warn "END_TASK: task with id '$data{ID}' from queue '$data{QUEUE}' doesn't exist in database\n";
            }
        } elsif ($data{MSG_TYPE} eq "DEL") {
            next if $data{ID} =~ /(all)|(-q)/; #what is id,-q,queue_name ; all ; all,-q,queue_name
            my @id = split /,/, $data{ID};

            foreach (@id) {
                if (exists $all_queues->{$data{QUEUE}}->{TASKS_HASH}->{$_}) {
                    my $cur_task = $all_queues->{$data{QUEUE}}->{TASKS_HASH}->{$_};
                    my $cur_user = $all_users->{$cur_task->{USER}};
                    my $cur_queue = $all_queues->{$cur_task->{QUEUE}};

                    $cur_task->{DEL} = 1;
                    $cur_user->{DELETED}++;
                    $cur_queue->{DELETED}++;
                } else {
                    warn "DEL: task with id '$_' from queue '$data{QUEUE}' doesn't exist in database\n";
                }
            }
        }
        $prev_pos = tell LOG;
    }
    $result;
}

#===  FUNCTION  ================================================================
#         NAME:  load_format
#   PARAMETERS:  filename
#      RETURNS:  format string
#===============================================================================
sub load_format {
    my $filename = shift;
    my $format;

    open FORMAT, '<', $filename or die "$0 : failed to open input file '$filename' : $!\n";

    while (<FORMAT>) {
        chomp ($format .= $_);
    }

    $format =~ s/\\n/\n/g;
    $format =~ s/\\t/\t/g;

    close FORMAT or warn "$0 : failed to close input file '$filename' : $!\n";

    $format;
}

#===  FUNCTION  ================================================================
#         NAME:  MAIN
#===============================================================================
my %data;

my $task_format_filename = "taskformat";
my $stat_format_filename = "statformat";

#---------------------------------------------------------------------------
#  process options
#---------------------------------------------------------------------------
my %options;

getopts ("kb:e:n:c:q:s:t:i:d:ruo:hp:", \%options);

my $dump_filename = "dump";
$dump_filename = $options{d} if ($options{d});

my $input_filename = $options{i} if ($options{i});

#---------------------------------------------------------------------------
#  option -h : usage
#---------------------------------------------------------------------------
if ($options{h}) {
    print <<EOF;
Usage:   clpr.pl [options]
Global Options:
          -h                                print this message
          -u                                update database
          -r                                reprocess log file and update database
          -i filename                       input (cleo log) file
          -d filename                       database file (default "./dump")
          -p u|q|t                          printing options:
                                                u - print users
                                                q - print queues
                                                t - print tasks
          -k                                print tasks (only when '-p u' or '-p q')
Filtering Options:
          -b dd:mm:yyyy hh:mm               begin time period (for time filter)
          -e dd:mm:yyyy hh:mm               end time period (for time filter)
          -n np_min-np_max                  np range
          -c cpuh_min-cpuh_max              cpuh_min, cpuh_max - floating point values
          -q queue1,..,queueN               queue list
          -s user1,..,userN                 users list
          -t run_time_min-run_time_max      run_time_* - run time range in seconds
EOF
    exit;
}

#---------------------------------------------------------------------------
#  option -u : update database 
#---------------------------------------------------------------------------
if ($options{u}) {
    exists $options{i} or die "you should specify input file name with -i option\n";

    %data = %{retrieve ($dump_filename)};

    open LOG, '<', $input_filename or die "$0 : failed to open input file '$input_filename' : $!\n";

    seek LOG, $data{LAST_POS}, 0;
    $_ = <LOG>;
    if ($_ ne $data{LAST}) {
        seek LOG, 0, 0;
    }

    %data = %{process_log *LOG, \%data};
    store \%data, "dump";

    close LOG or warn "$0 : failed to close input file '$input_filename' : $!\n";
}

#---------------------------------------------------------------------------
#  option -r : reprocess log
#---------------------------------------------------------------------------
if ($options{r}) {
    exists $options{i} or die "you should specify input file name with -i option\n";

    open LOG, '<', $input_filename or die "$0 : failed to open input file '$input_filename' : $!\n";

    %data = %{process_log *LOG, \%data};
    store \%data, "dump";

    print $data{LAST};

    close LOG or warn "$0 : failed to close input file '$input_filename' : $!\n";
}

#---------------------------------------------------------------------------
#  option -p : printing
#---------------------------------------------------------------------------
if ($options{p}) {
    #retreive stored data
    %data = %{retrieve ($dump_filename)} unless ($options{r} || $options{u});

    my ($begin_period, $end_period, $np_min, $np_max, $ch_min, $ch_max, $rt_min, $rt_max);
    my %user_list;
    my %queue_list;

    my $sp = sub {
        my $str = shift;
        if ($str =~ /(\d+):(\d+):(\d+)\s+(\d+):(\d+)/) {
            return mktime (0, $5, $4, $1, $2 - 1, $3 - 1900, 0, 0, -1);
        } else {
            die "wrong option parameter\n";
        }
    };

    #  period limitation
    $begin_period = $sp->($options{b}) if ($options{b});
    $end_period = $sp->($options{e}) if ($options{e});

    #  NP range
    if ($options{n}) {
        if ($options{n} =~ /(\d+)-(\d+)/) {
            $np_min = $1;
            $np_max = $2;
        } else {
            die "wrong option parameter\n";
        }
    }

    #  run time range
    if ($options{t}) {
        if ($options{t} =~ /(\d+)-(\d+)/) {
            $rt_min = $1;
            $rt_max = $2;
        } else {
            die "wrong option parameter\n";
        }
    }

    #  cpu hours range
    if ($options{c}) {
        if ($options{c} =~ /([0-9\.]+)-([0-9\.]+)/) {
            $ch_min = $1;
            $ch_max = $2;
        } else {
            die "wrong option parameter\n";
        }
    }

    #  users list
    if ($options{s}) {
        my @tmp = split /,/, $options{s};
        $user_list{$_} = 1 foreach (@tmp);
    }

    #  queue list
    if ($options{q}) {
        my @tmp = split /,/, $options{q};
        $queue_list{$_} = 1 foreach (@tmp);
    }

    #  read task format string
    my $task_format = load_format $task_format_filename;
    #  read stat format string
    my $stat_format = load_format $stat_format_filename;

    my @tasks = @{$data{TASKS}};

    sub time_predicate { exists $_[0]->{BEGIN_TIME} && $_[0]->{BEGIN_TIME} >= $_[1] && $_[0]->{BEGIN_TIME} <= $_[2]; }
    sub cpu_hours_predicate { exists $_[0]->{CPU_HOURS} && $_[0]->{CPU_HOURS} >= $_[1] && $_[0]->{CPU_HOURS} <= $_[2]; }
    sub queue_predicate { exists $_[1]->{$_[0]->{QUEUE}}; }
    sub user_predicate { exists $_[1]->{$_[0]->{USER}}; }
    sub np_predicate { exists $_->{NP} && $_->{NP} >= $_[1] && $_->{NP} <= $_[2]; }
    sub run_time_predicate { exists $_->{END_TIME} && exists $_->{BEGIN_TIME} && ($_->{END_TIME} - $_->{BEGIN_TIME}) >= $_[1] && ($_->{END_TIME} - $_->{BEGIN_TIME}) <= $_[2]; }

    @tasks = grep time_predicate ($_, $begin_period, $end_period), @tasks if defined $begin_period && defined $end_period;
    @tasks = grep np_predicate ($_, $np_min, $np_max), @tasks if defined $np_min && defined $np_max;
    @tasks = grep cpu_hours_predicate ($_, $ch_min, $ch_max), @tasks if defined $ch_min && defined $ch_max;
    @tasks = grep run_time_predicate ($_, $rt_min, $rt_max), @tasks if defined $rt_min && defined $rt_max;
    @tasks = grep user_predicate ($_, \%user_list), @tasks if %user_list;
    @tasks = grep queue_predicate ($_, \%queue_list), @tasks if %queue_list;

    $_->{USED} = 1 foreach (@tasks);
    
    if ($options{p} eq 't') {
        my $stat = collect_stat \@tasks;
        print_stat $stat, $stat_format;
        print_task $_, $task_format foreach (@tasks);
        exit;
    }

    if ($options{p} eq 'u') {
        my $all_users = $data{USERS};
        foreach (keys %$all_users) {
            next if (!exists $user_list{$_} && %user_list);

            my $tasks = $all_users->{$_}->{TASKS};
            my $stat = collect_stat $tasks;

            print "User name: $_\n";
            print_stat $stat, $stat_format;
            if ($options{k}) {
                foreach (@$tasks) {
                    print_task $_, $task_format if (defined $_->{USED});
                }
            }
        }
        exit;
    }

    if ($options{p} eq 'q') {
        my $all_queues = $data{QUEUES};
        foreach (keys %$all_queues) {
            next if (!exists $queue_list{$_} && $options{q});

            my $tasks = $all_queues->{$_}->{TASKS};
            my $stat = collect_stat $tasks;

            print "Queue name: $_\n";
            print_stat $stat, $stat_format;
            if ($options{k}) {
                foreach (@$tasks) {
                    print_task $_, $task_format if (defined $_->{USED});
                }
            }
        }
        exit;
    }
}
