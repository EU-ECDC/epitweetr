# creating package and installing it
export cdir=`pwd`
export OPENBLAS_NUM_THREADS=1
export SPARK_VERSION=3.1.2
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
export PATH=$PATH:$JAVA_HOME/bin

cd "$cdir/scala"
sbt package

mkdir -p "$cdir/epitweetr/inst/java"
cp "$cdir/scala/target/scala-2.12/ecdc-twitter-bundle_2.12-1.0.jar" "$cdir/epitweetr/inst/java"

git archive --format zip --output "$cdir/epitweetr/java/ecdc-twitter-bundle_2.12-1.0-source.zip" HEAD 

cd "$cdir/epitweetr"
Rscript ../devscripts/package.R

cd "$cdir"
