IFS=$'\n'; set -f
pattern=${1-\*.gz} 
for f in $(find data -type f -name $pattern)
  do 
  if gzip -cd $f >/dev/null ; then
    echo  -ne "OK: $f \033[0K\r"
  else 
    echo "KO $f"
  fi
done
unset IFS; set +f
