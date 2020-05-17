set cdir=%cd%
set OPENBLAS_NUM_THREADS=1
set SPARK_VERSION=2.4.5
set JAVA_HOME=C:\Program Files\AdoptOpenJDK\jdk-8.0.252.09-hotspot
set PATH=%PATH%;%JAVA_HOME%\bin
set HADOOP_HOME="c:/Users/pancho/github/ecdc-twitter-tool/epitweetr/inst"
set SBT_OPTS="-Xmx6G"
cd "%cdir%\scalaBridge"
call sbt assembly

if not exist "%cdir%\epitweetr\inst\java" mkdir "%cdir%\epitweetr\inst\java"
copy "%cdir%\scalaBridge\target\scala-2.11\ecdc-twitter-bundle-assembly-1.0.jar" "%cdir%\epitweetr\inst\java"

cd "%cdir%\epitweetr"

call Rscript ..\devscripts\package.R

cd %cdir%

