#!/bin/bash
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
send "epitweetr::detect_loop(\"/home/fod/datapub/epitweetr\")\r"
interact'
