SET CUR_CLASSPATH=%CLASSPATH%
SET CLASSPATH=lib\mysql-connector-java-5.1.6-bin.jar
SET CLASSPATH=%CLASSPATH%;.

javac -classpath %CLASSPATH% com\netflix\export\*.java

java -classpath %CLASSPATH% com.netflix.export.ExportMain export.properties

SET CLASSPATH=%CUR_CLASSPATH%
