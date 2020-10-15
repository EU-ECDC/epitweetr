# updating scala depedencies file
export cdir=`pwd`
export SPARK_VERSION=3.0.0

cd "$cdir/scala"

sbt package
cd "$cdir/scala/lib_managed"

find . -type f > "$cdir/epitweetr/inst/extdata/sbt-deps.txt"

cd "$cdir"

