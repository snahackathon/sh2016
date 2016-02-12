Run baseline: howto
==============

1. download data to `$DATA_HOME` 
2. install sbt (http://www.scala-sbt.org/)
3. clone the repository, then it might be opened in Idea 13+ as Scala sbt project.
The structure of the project is described here http://spark.apache.org/docs/latest/quick-start.html#self-contained-applications.
`git clone <repo>`
4. go to the directory where you cloned repository and enter `baseline` dir
`cd $PROJECT_HOME/baseline`
5. build project
`sbt package`
6. download spark (http://spark.apache.org/downloads.html). Recommended version spark-1.6.0-bin-hadoop2.6.tgz.
7. go to the directory where you downloaded spark
`cd $SPARK_HOME`
8. unpack spark
`spark tar -xvzf <spark>.tgz`
9. send jar you made in step 5 to spark (configuration is given for 4 cores)
``$SPARK_HOME/bin/spark-submit --class "Baseline" --master local[4] --driver-memory 4G $PROJECT_HOME/baseline/target/scala-2.10/baseline_2.10-1.0.jar $DATA_HOME``

