set cdir=%cd%
set OPENBLAS_NUM_THREADS=1
set SPARK_VERSION=2.4.5
set JAVA_HOME=C:\Program Files\AdoptOpenJDK\jdk-8.0.252.09-hotspot
set PATH=%PATH%;%JAVA_HOME%\bin

cd %cdir%\scalaBridge
sbt assembly

copy %cdir%\scalaBridge\target\scala-2.11\ecdc-twitter-bundle-assembly-1.0.jar %cdir%\epitweetr\java

cd %cdir%\epitweetr
Rscript ..\devscripts\package.R

cd %cdir%
