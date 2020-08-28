export cdir=`pwd`
export SPARK_VERSION=3.0.0

cd "$cdir/epitweetr/scala"

sbt package
cd "$cdir/epitweetr/scala/lib_managed"

find . -type f > "$cdir/epitweetr/inst/extdata/sbt-deps.txt"

cd "$cdir"

