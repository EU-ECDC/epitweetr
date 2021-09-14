#!/bin/bash
if [ -z ${EPI_HOME+x} ]; then echo "please set EPI_HOME is unset"; exit 1; fi
SCRIPT_PATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
pass=`pass epitools/ecdc_kr_pwd`
if [[ -z $pass ]] 
then
  pass insert epitools/ecdc_kr_pwd
  pass=`pass epitools/ecdc_kr_pwd`
fi

export ecdc_wtitter_tool_kr_password=$pass
export TMPDIR=$EPI_HOME/tmp
expect -c '
spawn R
expect ">"
send "reload <- function() {devtools::load_all(\"'$SCRIPT_PATH'/../epitweetr\");setup_config(\"'$EPI_HOME'\")}\r" 
send "reload()\r" 
interact'
