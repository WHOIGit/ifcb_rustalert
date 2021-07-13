#!/usr/bin/env bash

## CRONTAB ENTRY ##
# */30 * * * * /home/ifcb/github/ifcb_rust/run_rust.sh >> /home/ifcb/github/ifcb_rust/data/run_rust.sh.log 2>&1

echo "RUNNING run_rust.sh"
/usr/bin/date date -I'seconds'
cd "$(dirname "$0")"
PYTHON="/home/ifcb/miniconda3/envs/rust_demo/bin/python"
$PYTHON rust_hab.py --file input_args.txt
echo "------------------------------------------------------------------------"
