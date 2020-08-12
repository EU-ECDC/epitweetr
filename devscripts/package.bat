set cdir=%cd%
set OPENBLAS_NUM_THREADS=1
set SPARK_VERSION=2.4.5
set JAVA_HOME=C:\Program Files\AdoptOpenJDK\jdk-8.0.252.09-hotspot
set PATH=%PATH%;%JAVA_HOME%\bin
set SBT_OPTS="-Xmx6G"
cd "%cdir%\epitweetr\scala"
set TMP=C:\tools\tmp
call sbt package

if not exist "%cdir%\epitweetr\inst\java" mkdir "%cdir%\epitweetr\inst\java"
copy "%cdir%\epitweetr\scala\target\scala-2.11\ecdc-twitter-bundle_2.11-1.0.jar" "%cdir%\epitweetr\inst\java"

cd "%cdir%\epitweetr"

call Rscript ..\devscripts\package.R

cd %cdir%

