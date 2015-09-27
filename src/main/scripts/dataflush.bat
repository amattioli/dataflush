@echo off

set DATAFLUSH_HOME="%~dp0.."

set DATAFLUSH_JAVA_EXE="%JAVA_HOME%\bin\java.exe"

set CLI_MAIN="it.amattioli.dataflush.cli.Main"

setlocal EnableDelayedExpansion

if defined CLASSPATH_JAR (set CLASSPATH_JAR=%CLASSPATH_JAR%;.) else (set CLASSPATH_JAR=.)

for /R "%DATAFLUSH_HOME%\lib" %%G IN (*.jar) DO set CLASSPATH_JAR=!CLASSPATH_JAR!;%%G

%DATAFLUSH_JAVA_EXE% -classpath "%CLASSPATH_JAR%" %CLI_MAIN% %*