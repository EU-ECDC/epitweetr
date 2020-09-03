IFS=$'\n'; set -f
pattern=${1-\*2020.05\*.gz} 
for f in $(find $EPI_HOME/search/COVID-19/2020 -type f -name $pattern)
  do 
  if gzip -cd $f >/dev/null ; then
    echo  -ne "OK: $f \033[0K\r"
  else 
    echo "KO $f"
  fi
done
unset IFS; set +f
