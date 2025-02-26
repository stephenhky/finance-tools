#!/usr/bin/env bash

#SBATCH mem=5G

# This is a slurm script

STARTDATE=$1
ENDDATE=$2
CACHEDDIR=$3

python generating_yahoofinance_cache.py $STARTDATE $ENDDATE $CACHEDDIR