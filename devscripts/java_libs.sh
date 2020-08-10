export cdir=`pwd`
cd "$cdir/epitweetr/scala"

sbt package
cd "$cdir/epitweetr/scala/lib_managed"

find . -type f > "$cdir/epitweetr/inst/extdata/sbt-deps.txt"

cd "$cdir"

