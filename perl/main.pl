#!/usr/bin/perl 
#===============================================================================
#
#         FILE:  main.pl
#
#        USAGE:  ./main.pl  
#
#  DESCRIPTION:  main
#
#      OPTIONS:  ---
# REQUIREMENTS:  ---
#         BUGS:  ---
#        NOTES:  ---
#       AUTHOR:  Dmiry (), kondra2lp@gmail.com
#      COMPANY:  CMC MSU
#      VERSION:  0.1
#      CREATED:  08/11/2010 04:51:36 PM
#     REVISION:  ---
#===============================================================================

use strict;
use warnings;

use CleoParser;

my	$LOG_file_name = '../cleo-short.log';		# input file name

open LOG, '<', $LOG_file_name
    or die  "$0 : failed to open  input file '$LOG_file_name' : $!\n";

my $i = 0;

while (<LOG>) {
    parse_log_record $_;
}

close LOG
    or warn "$0 : failed to close input file '$LOG_file_name' : $!\n";

