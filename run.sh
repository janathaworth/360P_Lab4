#!/bin/bash


rm build/*
r:m output*/*
mkdir output5
hdfs dfs -rm -R output5
javac -classpath `hadoop classpath` -d build TextAnalyzer.java -Xlint
jar -cvf TextAnalyzer.jar -C build/ .
hadoop jar TextAnalyzer.jar TextAnalyzer /input output5
hdfs dfs -copyToLocal output5

