# You do not have to modify this file unless you have multiple packages in your MR java code
rm -r class/*
javac -classpath  $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar:$HADOOP_HOME/share/hadoop/common/hadoop-common-2.7.2.jar -d class code/*
jar -cvf CalculateAverage.jar -C class/ .
