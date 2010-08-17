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

    my %res;
    my %mounths = ("Jan", 0, "Feb", 1, "Mar", 2, "Apr", 3, "May", 4, "Jun", 5, "Jul", 6, "Aug", 7, "Sep", 8, "Oct", 9, "Nov", 10, "Dec", 11);

    if (/\[(\w+)\s(\w+)\s(\d+)\s(\d+):(\d+):(\d+)\s(\d+)\]/) {
        $res{TIME} = mktime($6, $5, $4, $3, $mounths{$2}, $7 - 1900, 0, 0, -1);
    }

    my $req_queue = $1 if ($_[0] =~ /\]\s(\w+)\s/);

    my @words = split /;\s|;;\s/, $_;

    my $msg_type = $1 if (/:([A-Za-z_0]+)\s/);
    $res{MSG_TYPE} = $msg_type;

    if ($msg_type eq "ADD") {
        $res{USER} = $1 if ($words[0] =~ /\s(\w+)$/);
        $res{QUEUE} = $words[1];
        $res{NP} = $words[2];
    } elsif ($msg_type eq "ADDED") {
        $res{id} = $1 if ($words[0] =~ /\s(\w+)$/);
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
    } elsif ($msg_type eq "END_TASK0") {
        $res{ID} = $1 if ($words[0] =~ /\s(\w+)$/);
    } elsif ($msg_type eq "END_TASK") {
        $res{ID} = $1 if ($words[0] =~ /\s(\w+)$/);
        $res{USER} = $words[1];
        $res{STATUS} = $words[2];
        $res{SIGNAL} = $words[3];
    }
    return %res;
}

