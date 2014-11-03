#!/bin/bash

COMPACT_EXT=`dirname $0`/compact-stat.awk;



TDIR=$1;

echo "p.out--------------------------"
cat $TDIR/p.out| grep -A13 db_bench
echo ""
echo ""
echo "sde.out------------------------"
cat $TDIR/sde.out| grep -A6 Total
echo ""
echo ""
echo "md0.out------------------------"
cat $TDIR/md0.out| grep -A6 Total
echo ""
echo ""
echo "hlsm_log-----------------------"
cat $TDIR/hlsm_log| grep Counter
echo ""
echo ""
echo "LOG----------------------------"
awk -f $COMPACT_EXT -v info=basic -v title=1 < $TDIR/LOG
