InputFile="twitter_dist_1.txt"
InputDir="homework2/data"
OutputDir="homework2/part1/output_twitter_dist_1"
#InputFile="test.txt"
#InputDir="homework2/data"
#OutputDir="homework2/part1/output_test_1"
source="1173"
#source="1"
iteration="5"
#option="weighted"
option="nonweighted"
hadoop com.sun.tools.javac.Main PDPreProcess.java PDNodeWritable.java ParallelDijkstra.java
jar cf ParallelDijkstra.jar ParallelDijkstra*.class PDNodeWritable.class  PDPreProcess*.class
hdfs dfs -rmr $OutputDir
hadoop jar ParallelDijkstra.jar ParallelDijkstra $InputDir/$InputFile $OutputDir $source $iteration $option

