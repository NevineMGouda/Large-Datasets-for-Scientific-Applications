1- To run the code for Task I: Counting the occurrences of each First letter do the following:
  - Given that the Hadoop cluster is setup successfully
    AND the FirstLetterCount.java (provided) is located in the same directory of wordcount folder ("/home/ubuntu/wordcount/")
    To upload the dataset in HDFS:
      1. /usr/local/hadoop/bin/hdfs dfs -mkdir /user
      2. /usr/local/hadoop/bin/hdfs dfs -mkdir /user/ubuntu
      3. /usr/local/hadoop/bin/hdfs dfs -put /home/ubuntu/wordcount/input /user/ubuntu/WordCountInput

    To compile, build and run the code:
      1. javac -cp `/usr/local/hadoop/bin/hadoop classpath` FirstLetterCount.java
      2. jar -cvf FirstLetterCount.jar *.class
      3. /usr/local/hadoop/bin/hadoop jar FirstLetterCount.jar FirstLetterCount /user/ubuntu/WordCountInput /user/ubuntu/FirstLetterCountOutput
      4. /usr/local/hadoop/bin/hdfs dfs -ls /user/ubuntu/FirstLetterCountOutput
      5. /usr/local/hadoop/bin/hdfs dfs -cat /user/ubuntu/FirstLetterCountOutput/part-00000


2- To run the code for Task II: Analyzing Twitter Data using Hadoop streaming and Python:
  - Given that the Hadoop cluster is setup successfully
    AND the mapper.py and reducer.py (provided) are located in the same directory of tweets folder ("/home/ubuntu/tweets/")
    To upload the dataset in HDFS:
      1. /usr/local/hadoop/bin/hdfs dfs -mkdir /user
      2. /usr/local/hadoop/bin/hdfs dfs -mkdir /user/ubuntu
      3. /usr/local/hadoop/bin/hdfs dfs -put / /user/ubuntu/Tweets
      /usr/local/hadoop/bin/hdfs dfs -put /home/ubuntu/tweets/input /user/ubuntu/tweetsInput

    To compile, build and run the code:
      1. /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.0.jar -file /home/ubuntu/tweets/mapper.py    -mapper /home/ubuntu/tweets/mapper.py -file /home/ubuntu/tweets/reducer.py   -reducer /home/ubuntu/tweets/reducer.py -input /user/ubuntu/tweetsInput/ -output /user/ubuntu/tweetsOutput
      2. /usr/local/hadoop/bin/hdfs dfs -ls /user/ubuntu/tweetsOutput
      3. /usr/local/hadoop/bin/hdfs dfs -cat /user/ubuntu/tweetsOutput/part-00000


3- To run the code for Task III: Analyzing Twitter Data using HIVE:
  - Given that the Hadoop cluster is setup successfully
    AND the tweets are still available from last step at the HDFS we just need to run the code
    AND HADOOP_HOME and HIVE_HOME variables are set

    To compile, build and run the code:
      1. $HIVE_HOME/bin/hive -f Task3.hql
