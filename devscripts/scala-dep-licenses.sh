export cdir=`pwd`
export OPENBLAS_NUM_THREADS=1
export SPARK_VERSION=3.0.0
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
export PATH=$PATH:$JAVA_HOME/bin

cd "$cdir/scala"
sbt dumpLicenseReport

cp "$cdir/epitweetr/target/license-reports/"* "$cdir/licenses/scala-licenses/"

cd "$cdir"
