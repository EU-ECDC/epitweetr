#!/bin/bash
if [ -z ${EPI_HOME+x} ]; then echo "please set EPI_HOME is unset"; exit 1; fi
pass=`pass epitools/ecdc_kr_pwd`
if [[ -z $pass ]] 
then
  pass insert epitools/ecdc_kr_pwd
  pass=`pass epitools/ecdc_kr_pwd`
fi

export ecdc_wtitter_tool_kr_password=$pass
expect -c '
spawn R
expect ">"
send "epitweetr::setup_config(\"'$EPI_HOME'\")\r" 
send "epitweetr::search_loop()\r"
interact'
