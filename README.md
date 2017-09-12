# Literature Recommender
Recommend articles to researchers based on item collaborative filtering algorithm.

### Highlight
Construct literature similarity matrix and citation matrix, and calculate matrix multiplication based on Hadoop MapReduce.

### Build
- clone the repo `git clone https://github.com/lucia1219cs/Literature-Recommender.git`
- copy the input file to HDFS `hdfs dfs -mkdir /input` `hdfs dfs -put input/* /input`
- `cd src/main/java/` 
- compile `hadoop com.sun.tools.javac.Main *.java`
- create a jar `jar cf recommender.jar *.class`
- run MapReduce jobs `hadoop jar recommender.jar Driver /input /DataDivider /SimilarityMatrix /Normalize /Multiplication /Sum`
- view results `hdfs dfs -cat /Sum/*`
