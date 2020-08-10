export cdir=`pwd`
export OPENBLAS_NUM_THREADS=1
export SPARK_VERSION=2.4.5
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
export PATH=$PATH:$JAVA_HOME/bin

cd "$cdir/epitweetr/scala"
sbt package

mkdir -p "$cdir/epitweetr/inst/java"
cp "$cdir/epitweetr/scala/target/scala-2.11/ecdc-twitter-bundle_2.11-1.0.jar" "$cdir/epitweetr/inst/java"

cd "$cdir/epitweetr"
Rscript ../devscripts/package.R

cd "$cdir"
