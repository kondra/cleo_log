#!/usr/bin/perl 

use strict;
use warnings;

use Storable qw(store retrieve);
use YAML qw(Dump);
use POSIX;

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

sub print_task {
    my $cur_task = shift;
    my $format = shift;

    print "\tid: ", $cur_task->{ID}, "\n" if $format =~ /%i/;
    print "\tqueue: ", $cur_task->{QUEUE}, "\n" if $format =~ /%q/;
    print "\tuser: ", $cur_task->{USER}, "\n" if $format =~ /%u/;
    print "\tsignal: ", $cur_task->{SIGNAL}, "\n" if exists $cur_task->{SIGNAL} && $format =~ /%s/;
    print "\tstatus: ", $cur_task->{STATUS}, "\n" if exists $cur_task->{STATUS} && $format =~ /%t/;
    print "\tnp: ", $cur_task->{NP}, "\n" if $format =~ /%n/;
    print "\tnp extra: ", $cur_task->{NP_EXTRA}, "\n" if exists $cur_task->{NP_EXTRA} && $format =~ /%x/;
    print "\tcpu hours: ", $cur_task->{CPU_HOURS}, "\n" if exists $cur_task->{CPU_HOURS} && $format =~ /%c/;
    printf "\tcpu hours: %.2lf\n", $cur_task->{CPU_HOURS} / 3600 if exists $cur_task->{CPU_HOURS} && $format =~ /%c/;
    if (exists $cur_task->{RUN_TIME} && $format =~ /%r/) {
        my $hh = $cur_task->{RUN_TIME} / 3600;
        my $mm = ($cur_task->{RUN_TIME} % 3600) / 60;
        my $ss = ($cur_task->{RUN_TIME} % 3600) % 60;
        printf "\trun time: %02.0lf:%02.0lf:%02.0lf\n", $hh, $mm, $ss;
    }
    print "\tadded time: ", ctime ($cur_task->{ADDED_TIME}) if $format =~ /%a/;
    print "\tbegin time: ", ctime ($cur_task->{BEGIN_TIME}) if exists $cur_task->{BEGIN_TIME} && $format =~ /%b/;
    print "\tend time: ", ctime ($cur_task->{END_TIME}) if exists $cur_task->{END_TIME} && $format =~ /%e/;
    print "\n" if $format =~ /%[iqustnxabec]/;
}

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

sub time_predicate {
    return exists $_[0]->{BEGIN_TIME} && $_[0]->{BEGIN_TIME} >= $_[1] && $_[0]->{BEGIN_TIME} <= $_[2];
}

sub cpu_hours_predicate {
    return exists $_[0]->{CPU_HOURS} && $_[0]->{CPU_HOURS} >= $_[1] && $_[0]->{CPU_HOURS} <= $_[2];
}

sub queue_predicate {
    return exists $_[1]->{$_[0]->{QUEUE}};
}

sub user_predicate {
    return exists $_[1]->{$_[0]->{USER}};
}

sub np_predicate {
    return exists $_->{NP} && $_->{NP} >= $_[1] && $_->{NP} <= $_[2];
}

sub run_time_predicate {
    return exists $_->{END_TIME} && exists $_->{BEGIN_TIME}
            && ($_->{END_TIME} - $_->{BEGIN_TIME}) >= $_[1] && ($_->{END_TIME} - $_->{BEGIN_TIME}) <= $_[2];
}

sub process_log {
    my $filename = shift;

    open LOG, '<', $filename
        or die "$0 : failed to open input file '$filename' : $!\n";

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

    close LOG
        or warn "$0 : failed to close input file '$filename' : $!\n";

    my %result;
    $result{USERS} = \%all_users;
    $result{QUEUES} = \%all_queues;
    $result{TASKS} = \@all_tasks;
    $result{LAST} = $_;

    return %result;
}

my $file_name = '../cleo-short.log';		# input file name

#my %data = process_log $file_name;

#store (\%data, 'dump');

my $data = retrieve ('dump');
#print_users $data->{USERS}, "%A%S%H%W%S%U%K";
#print_queues $data->{QUEUES}, "%T%r%i%q%u%s%t%n%x%c%a%b%e";
#print_queues $data{QUEUES}, "%T%r%i%q%u%s%t%n%x%c%a%b%e";
open my($f), ">", 'yml';
print $f, Dump($data->{USERS});
