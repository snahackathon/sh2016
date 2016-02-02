Run baseline: howto
==============

1. install sbt (http://www.scala-sbt.org/)
2. clone the repository, then it might be opened in Idea 13.x as Scala sbt project.
The structure of the project is described here http://spark.apache.org/docs/latest/quick-start.html#self-contained-applications.
`git clone <repo>`
3. go to the directory where you cloned repository and enter `baseline` dir
`cd $PROJECT_HOME/baseline`
4. build project
`sbt package`
5. download spark (http://spark.apache.org/downloads.html)
6. go to the directory where you downloaded spark
`cd $SPARK_HOME`
7. unpack spark
`spark tar -xvzf <spark>.tgz`
8. send jar you made in step 4 to spark (configuration is given for 4 cores)
``$SPARK_HOME/spark-submit --class "Baseline" --master local[4] --driver-memory 4G $PROJECT_HOME/baseline/target/scala-2.10/baseline_2.10-1.0.jar``

