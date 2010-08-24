#!/usr/bin/perl 

use strict;
use warnings;

use String::Format;#  qw(stringf);
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
    return %res;
}

#===  FUNCTION  ================================================================
#         NAME:  print_task
#   PARAMETERS:  task hash pointer
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
    return;
}

#    my $cur_task = shift;
#    my $format = shift;
#
#    print "\tid: ", $cur_task->{ID}, "\n" if $format =~ /%i/;
#    print "\tqueue: ", $cur_task->{QUEUE}, "\n" if $format =~ /%q/;
#    print "\tuser: ", $cur_task->{USER}, "\n" if $format =~ /%u/;
#    print "\tsignal: ", $cur_task->{SIGNAL}, "\n" if exists $cur_task->{SIGNAL} && $format =~ /%s/;
#    print "\tstatus: ", $cur_task->{STATUS}, "\n" if exists $cur_task->{STATUS} && $format =~ /%t/;
#    print "\tnp: ", $cur_task->{NP}, "\n" if $format =~ /%n/;
#    print "\tnp extra: ", $cur_task->{NP_EXTRA}, "\n" if exists $cur_task->{NP_EXTRA} && $format =~ /%x/;
#    printf "\tcpu hours: %.2lf\n", $cur_task->{CPU_HOURS} / 3600 if exists $cur_task->{CPU_HOURS} && $format =~ /%c/;
#    if (exists $cur_task->{RUN_TIME} && $format =~ /%r/) {
#        my $hh = $cur_task->{RUN_TIME} / 3600;
#        my $mm = ($cur_task->{RUN_TIME} % 3600) / 60;
#        my $ss = ($cur_task->{RUN_TIME} % 3600) % 60;
#        printf "\trun time: %02.0lf:%02.0lf:%02.0lf\n", $hh, $mm, $ss;
#    }
#    print "\tadded time: ", ctime ($cur_task->{ADDED_TIME}) if $format =~ /%a/;
#    print "\tbegin time: ", ctime ($cur_task->{BEGIN_TIME}) if exists $cur_task->{BEGIN_TIME} && $format =~ /%b/;
#    print "\tend time: ", ctime ($cur_task->{END_TIME}) if exists $cur_task->{END_TIME} && $format =~ /%e/;
#    print "\n" if $format =~ /%[iqustnxabec]/;

sub print_stat {
    my $stat = shift;
    my $format = shift;

    print "stat: \n";
    printf "\tcpu hours: %.2lf\n", $stat->{CPU_HOURS} / 3600 if exists $stat->{CPU_HOURS} && $format =~ /%H/;
    if (exists $stat->{WAIT_TIME} && $format =~ /%W/) {
        my $hh = $stat->{WAIT_TIME} / 3600;
        my $mm = ($stat->{WAIT_TIME} % 3600) / 60;
        my $ss = ($stat->{WAIT_TIME} % 3600) % 60;
        printf "\twait time: %02.0lf:%02.0lf:%02.0lf\n", $hh, $mm, $ss;
    }
    my $cnt = @{$stat->{TASKS}};
    print "\ttotal task count: ", $cnt, "\n" if $format =~ /%A/;
    print "\tsucceded tasks: ", $stat->{SUCCEDED}, "\n" if exists $stat->{SUCCEDED} && $format =~ /%S/;
    print "\tunsucceded tasks: ", $stat->{UNSUCCEDED}, "\n" if exists $stat->{UNSUCCEDED} && $format =~ /%U/;
    print "\tkilled tasks: ", $stat->{KILLED}, "\n" if exists $stat->{KILLED} && $format =~ /%K/;
}

sub print_users {
    my $all_users = shift;
    my $format = shift;

    my $cnt = keys %{$all_users};
    print "users count: $cnt\n";

    foreach my $cur_user (keys %{$all_users}) {
        my $user = $all_users->{$cur_user};
        print "user: $cur_user\n";
        if ($format =~ /%S/) {
            print_stat $user, $format;
        }
        if ($format =~ /%T/) {
            print "tasks:\n";
            foreach my $cur_task (@{$all_users->{$cur_user}->{TASKS}}) {
                print_task $cur_task, $format;
            }
        }
        print "\n";
    }
}

sub print_queues {
    my $all_queues = shift;
    my $format = shift;

    my $cnt = keys %{$all_queues};
    print "queues count: $cnt\n";

    foreach my $cur_queue (keys %{$all_queues}) {
        print "queue: $cur_queue\n";
        if ($format =~ /%T/) {
            print "tasks:\n";
            foreach my $cur_task (@{$all_queues->{$cur_queue}->{TASKS}}) {
                print_task $cur_task, $format;
            }
        }
        print "\n";
    }
}

sub collect_stat {
    my $tasks = shift;
    my %stat;

    $stat{TASKS} = $tasks;
    foreach my $cur_task (@$tasks) {
        $stat{CPU_HOURS} += $cur_task->{CPU_HPURS};
        $stat{SUCCEDED}++ if ($cur_task->{SIGNAL} == 0 && $cur_task->{SIGNAL} == 0);
        $stat{UNSUCCEDED}++ if ($cur_task->{SIGNAL} || $cur_task->{SIGNAL});
        $stat{KILLED}++ if ($cur_task->{SIGNAL});
    }
    return \%stat;
}

sub time_predicate { exists $_[0]->{BEGIN_TIME} && $_[0]->{BEGIN_TIME} >= $_[1] && $_[0]->{BEGIN_TIME} <= $_[2]; }

sub cpu_hours_predicate { exists $_[0]->{CPU_HOURS} && $_[0]->{CPU_HOURS} >= $_[1] && $_[0]->{CPU_HOURS} <= $_[2]; }

sub queue_predicate { exists $_[1]->{$_[0]->{QUEUE}}; }

sub user_predicate { exists $_[1]->{$_[0]->{USER}}; }

sub np_predicate { exists $_->{NP} && $_->{NP} >= $_[1] && $_->{NP} <= $_[2]; }

sub run_time_predicate { exists $_->{END_TIME} && exists $_->{BEGIN_TIME} && ($_->{END_TIME} - $_->{BEGIN_TIME}) >= $_[1] && ($_->{END_TIME} - $_->{BEGIN_TIME}) <= $_[2]; }

#===  FUNCTION  ================================================================
#         NAME:  process_log
#      PURPOSE:  main processing function
#   PARAMETERS:  link to file descriptor
#      RETURNS:  hash with tasks, users and queues
#===============================================================================
sub process_log {
    local *LOG = shift;

    my @all_tasks;
    my %all_queues;
    my %all_users;

    my $i = 0;
    while (<LOG>) {
        $i++;
        my %cur_task;
        my %data = parse_log_record $_;
        if ($data{MSG_TYPE} eq "ADDED") {
            $cur_task{ID} = $data{ID};
            $cur_task{USER} = $data{USER};
            $cur_task{QUEUE} = $data{QUEUE};
# what is "1-as" "1-"
            if ($data{NP} =~ /(\d+)-/) {
                $cur_task{NP} = $1;
            } else {
                $cur_task{NP} = $data{NP};
            }
#
            $cur_task{ADDED_TIME} = $data{TIME};

            push @all_tasks, \%cur_task;
            
            if (exists $all_queues{$cur_task{QUEUE}}) {
                push @{$all_queues{$cur_task{QUEUE}}->{TASKS}}, \%cur_task;
                $all_queues{$cur_task{QUEUE}}->{TASKS_HASH}->{$cur_task{ID}} = \%cur_task;
            } else {
                my %new_queue;
                my @queue_tasks;
                my %queue_tasks_hash;
                push @queue_tasks, \%cur_task; 
                $queue_tasks_hash{$cur_task{ID}} = \%cur_task;
                $new_queue{TASKS} = \@queue_tasks;
                $new_queue{TASKS_HASH} = \%queue_tasks_hash;
                $all_queues{$cur_task{QUEUE}} = \%new_queue;
            }

            if (exists $all_users{$cur_task{USER}}) {
                push @{$all_users{$cur_task{USER}}->{TASKS}}, \%cur_task;
            } else {
                my %new_user;
                my @user_tasks;
                push @user_tasks, \%cur_task;
                $new_user{TASKS} = \@user_tasks;
                $all_users{$cur_task{USER}} = \%new_user;
            }
        } elsif ($data{MSG_TYPE} eq "RUN_NODES") {
            if (exists $all_queues{$data{QUEUE}}->{TASKS_HASH}->{$data{ID}}) {
                my $cur_task = $all_queues{$data{QUEUE}}->{TASKS_HASH}->{$data{ID}};
                $cur_task->{BEGIN_TIME} = $data{TIME};
                $cur_task->{NP_EXTRA} = $data{NP_EXTRA};

                my $cur_user = $all_users{$cur_task->{USER}};
                $cur_user->{WAIT_TIME} += $cur_task->{BEGIN_TIME} - $cur_task->{ADDED_TIME};
            } else {
#                warn "task with id '$data{ID}' from queue '$data{QUEUE}' doesn't exist in database\n";
            }
        } elsif ($data{MSG_TYPE} eq "END_TASK") {
            if (exists $all_queues{$data{QUEUE}}->{TASKS_HASH}->{$data{ID}}) {
                my $cur_task = $all_queues{$data{QUEUE}}->{TASKS_HASH}->{$data{ID}};
                $cur_task->{END_TIME} = $data{TIME};
                $cur_task->{SIGNAL} = $data{SIGNAL};
                $cur_task->{STATUS} = $data{STATUS};
                $cur_task->{RUN_TIME} = $cur_task->{END_TIME} - $cur_task->{BEGIN_TIME};

                my $ch = ($cur_task->{END_TIME} - $cur_task->{BEGIN_TIME}) * $cur_task->{NP};
                $cur_task->{CPU_HOURS} = $ch;

                my $cur_user = $all_users{$cur_task->{USER}};
                my $cur_queue = $all_queues{$cur_task->{QUEUE}};

                $cur_user->{CPU_HOURS} += $ch;
                $cur_queue->{CPU_HOURS} += $ch;

                $cur_queue->{SUCCEDED}++, $cur_user->{SUCCEDED}++ if ($cur_task->{SIGNAL} == 0 && $cur_task->{SIGNAL} == 0);
                $cur_queue->{UNSUCCEDED}++, $cur_user->{UNSUCCEDED}++ if ($cur_task->{SIGNAL} || $cur_task->{SIGNAL});
                $cur_queue->{KILLED}++, $cur_user->{KILLED}++ if ($cur_task->{SIGNAL});
            } else {
#                warn "task with id '$data{ID}' from queue '$data{QUEUE}' doesn't exist in database\n";
            }
        }
    }

    my %result;
    $result{USERS} = \%all_users;
    $result{QUEUES} = \%all_queues;
    $result{TASKS} = \@all_tasks;
    $result{LAST} = $_;

    return %result;
}


#===  FUNCTION  ================================================================
#         NAME:  MAIN
#===============================================================================
my %data;

my $dump_filename = "dump";
my $format_filename = "format";

#---------------------------------------------------------------------------
#  process options
#---------------------------------------------------------------------------
my %options;

getopts ("f:i:d:ruo:hp:", \%options);

#---------------------------------------------------------------------------
#  option -i : input log file name
#---------------------------------------------------------------------------
my $input_filename = $options{i} if ($options{i});

#---------------------------------------------------------------------------
#  option -f : format string filename
#---------------------------------------------------------------------------
$format_filename = $options{f} if ($options{f});

#---------------------------------------------------------------------------
#  option -p : printing options
#---------------------------------------------------------------------------
my $print = $options{p} if ($options{p});

#---------------------------------------------------------------------------
#  option -d : binary dump file name
#---------------------------------------------------------------------------
$dump_filename = $options{d} if ($options{d});

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
          -f filename                       filename with format strings for task, queue and user stats (default "./format")
          -i filename                       input (cleo log) file
          -d filename                       database file (default "./dump")
          -p u|q|t                          printing options:
                                                u - print users
                                                q - print queues
                                                t - print tasks
Filtering Options:
          -b dd:mm:yyyy hh:mm               begin time period (for time filter)
          -e dd:mm:yyyy hh:mm               end time period (for time filter)
          -n np_min-np_max                  np range
          -c cpuh_min-cpuh_max              cpuh_min, cpuh_max - floating point values
          -qaqueue1,..,queueN               queue list
          -s user1,..,userN                 users list
          -t run_time_min-run_time_max      run_time_* - run time range in seconds
EOF
    exit;
}

#---------------------------------------------------------------------------
#  option -u : update database 
#---------------------------------------------------------------------------
if ($options{u}) {
    %data = %{retrieve ($dump_filename)};
}

#---------------------------------------------------------------------------
#  option -r : reprocess log
#---------------------------------------------------------------------------
if ($options{r}) {
    unless (exists $options{i}) {
        die "you should specify input file name with -i option\n";
    }

    open LOG, '<', $input_filename
        or die "$0 : failed to open input file '$input_filename' : $!\n";

    %data = process_log *LOG;
    store \%data, "dump";

close LOG
    or warn "$0 : failed to close input file '$input_filename' : $!\n";
}

#---------------------------------------------------------------------------
#  option -p : printing
#---------------------------------------------------------------------------
if ($options{p}) {
    my $begin_period = 0;
    my $end_period = 2000000000;
    my $np_min = 0;
    my $np_max = 1000000;
    my $ch_min = 0;
    my $ch_max = 20000000000;
    my $rt_min = 0;
    my $rt_max = 20000000000;
    my @user_list;
    my @queue_list;
    my $format;

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
        @user_list = split /,/, $options{s};
    }

    #  queue list
    if ($options{q}) {
        @queue_list = split /,/, $options{q};
    }

    #  read format string
    open FORMAT, '<', $format_filename
        or die "$0 : failed to open input file '$format_filename' : $!\n";

    while (<FORMAT>) {
        chomp ($format .= $_);
    }

    $_ = $format;
    s/\\n/\n/g;
    s/\\t/\t/g;
    $format = $_;
    
# or like this
# my $format = "Task info:\n\tid: %i\n\tqueue: %q\n\tuser: %u\n\tsignal: %s\n\tstatus: %t\n\tnp: %n\n\tnp extra: %x\n\tcpu hours: %.2c\n\trun time: %r\n\tadded time: %a\tbegin time: %b\tend time: %e\n";

    close FORMAT
        or warn "$0 : failed to close input file '$format_filename' : $!\n";

#    foreach my $cur_task ($data->{TASKS}) {
#        print_task $cur_task, $format;
#    }
    my $all_users = $data{USERS};
    foreach my $cur_task (@{$all_users->{burunduk}->{TASKS}}) {
        print_task $cur_task, $format;
    }
}


#my %data = process_log $file_name;
#store (\%data, 'dump');
#my $data = retrieve ('dump');
#print_users $data->{USERS}, "%A%S%H%W%S%U%K";
#print_queues $data->{QUEUES}, "%T%r%i%q%u%s%t%n%x%c%a%b%e";
#print_queues $data{QUEUES}, "%T%r%i%q%u%s%t%n%x%c%a%b%e";
#print Dump($data->{USERS});
