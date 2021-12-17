# updating scala depedencies file
export cdir=`pwd`
export SPARK_VERSION=3.0.3

cd "$cdir/scala"

rm -r "$cdir/scala/lib_managed"
sbt package
cd "$cdir/scala/lib_managed"

find . -type f > "$cdir/epitweetr/inst/extdata/sbt-deps.txt"

cd "$cdir"

