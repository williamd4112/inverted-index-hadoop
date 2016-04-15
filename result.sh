# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/CalculateAverage/Output/
#hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage /user/shared/CalculateAverage/Input /user/TA/CalculateAverage/Output
#hdfs dfs -cat /user/TA/CalculateAverage/Output/part-*

file_directory=input
input_directory=retrieval_result
your_hadoop_output_directory=sorted_result

hdfs dfs -rm -r $your_hadoop_output_directory
hadoop jar InvertedIndex.jar calculateAverage.Driver $input_directory $your_hadoop_output_directory rs $file_directory
hdfs dfs -cat $your_hadoop_output_directory/part-*
rm -r $your_hadoop_output_directory
hdfs dfs -get $your_hadoop_output_directory .
