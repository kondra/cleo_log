#
#===============================================================================
#
#         FILE:  CleoParser.pm
#
#  DESCRIPTION:  cleo log parser module
#
#        FILES:  ---
#         BUGS:  ---
#        NOTES:  ---
#       AUTHOR:  Dmiry (), kondra2lp@gmail.com
#      COMPANY:  CMC MSU
#      VERSION:  0.1
#      CREATED:  08/11/2010 09:39:40 PM
#     REVISION:  ---
#===============================================================================

package CleoParser;

use strict;
use warnings;

use base 'Exporter';

use POSIX;

our $VERSION = '0.1';
our @EXPORT = qw(parse_log_record);

sub parse_log_record {
    $_ = $_[0];

    my ($time, $user, $queue, $np, $id, $signal, $status, $np_extra);

    my %mounths = ("Jan", 0, "Feb", 1, "Mar", 2, "Apr", 3, "May", 4, "Jun", 5, "Jul", 6, "Aug", 7, "Sep", 8, "Oct", 9, "Nov", 10, "Dec", 11);

    if (/\[(\w+)\s(\w+)\s(\d+)\s(\d+):(\d+):(\d+)\s(\d+)\]/) {
        $time = mktime($6, $5, $4, $3, $mounths{$2}, $7 - 1900, 0, 0, -1);
    }

    my $req_queue = $1 if ($_[0] =~ /\]\s(\w+)\s/);

    my @words = split /;\s|;;\s/, $_;

    my $msg_type = $1 if (/:([A-Za-z_0]+)\s/);

    if ($msg_type eq "ADD") {
        $user = $1 if ($words[0] =~ /\s(\w+)$/);
        $queue = $words[1];
        $np = $words[2];
    } elsif ($msg_type eq "ADDED") {
        $id = $1 if ($words[0] =~ /\s(\w+)$/);
        $queue = $words[1];
        if (/;;/) {
            $user = $words[2];
            $np = $words[3];
        } else {
            $user = $words[3];
            $np = $words[4];
        }
    } elsif ($msg_type eq "DEL") {
        $user = $1 if ($words[0] =~ /\s(\w+)$/);
        $queue = $words[1];
        chomp ($id = $words[2]);
    } elsif ($msg_type eq "RUN_NODES") {
        $id = $1 if ($words[0] =~ /\s(\w+)$/);
        $user = $words[1];
        $np = $words[2];
        $np_extra = $words[3];
    } elsif ($msg_type eq "END_TASK0") {
        $id = $1 if ($words[0] =~ /\s(\w+)$/);
    } elsif ($msg_type eq "END_TASK") {
        $id = $1 if ($words[0] =~ /\s(\w+)$/);
        $user = $words[1];
        $status = $words[2];
        $signal = $words[3];
    }
}

