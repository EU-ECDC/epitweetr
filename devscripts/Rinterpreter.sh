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
send "reload <- function() {devtools::load_all(\"/home/fod/github/ecdc-twitter-tool/epitweetr\");setup_config(\"/media/fod/Bluellet/datapub/epitweetr\")}\r" 
send "reload()\r" 
interact'
