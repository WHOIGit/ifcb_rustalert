#!/usr/bin/env bash

## CRONTAB ENTRY ##
# */30 * * * * /home/ifcb/github/ifcb_rust/rustalert.sh >> /home/ifcb/github/ifcb_rust/data/rustalert.sh.log 2>&1

echo "RUNNING rustalert.sh"
/usr/bin/date -I'seconds'
cd "$(dirname "$0")"
PYTHON="/home/ifcb/miniconda3/envs/rust/bin/python"
$PYTHON rustalert.py --file params.txt
echo "------------------------------------------------------------------------"
