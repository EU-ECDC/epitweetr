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
send "packageName <- \"epitweetr\"\r" 
send "devtools::load_all(\"/home/fod/github/ecdc-twitter-tool/epitweetr\")\r"
send "epitweetr::setup_config(\"/home/fod/github/ecdc-twitter-tool/data\")\r" 
send "epitweetr::epitweetr_app()\r"
interact'
