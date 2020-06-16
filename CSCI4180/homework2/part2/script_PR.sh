InputFile="twitter_dist_2.txt"
InputDir="homework2/data"
OutputDir="homework2/part2/output_twitter_dist_2"
#InputFile="test_PR.txt"
#InputDir="homework2/data"
#OutputDir="homework2/part2/output_test_1"
alpha="0.123"
iteration="5"
hadoop com.sun.tools.javac.Main PageRank.java PRPreProcess.java PRNodeWritable.java PRAdjust.java 
jar cf PageRank.jar PRPreProcess*.class PRNodeWritable.class PageRank*.class PRAdjust*.class
hdfs dfs -rmr $OutputDir
hadoop jar PageRank.jar PageRank $alpha $iteration $InputDir/$InputFile $OutputDir 

