Tools
====

##compact-stat.awk
###Usage 1
Get the statistics information about the compaction in the LOG.
```bash
awk -f compact-stat.awk -v info=basic -v title=1 < LOG
```

###Usage 2
Get the distribution of number of files involved in compactions. 
```bash
awk -f compact-stat.awk -v info=fdist < LOG
```
