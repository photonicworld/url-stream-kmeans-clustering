# README #

### How do I get set up? ###
* Summary of set up
    1. Setup sbt with ```brew install sbt```.
    2. Make sure sbt is installed by checking ```sbt about```.
    3. From the root directory of this project run ```sbt``` to open up sbt console.
    4. ```> compile``` in the sbt console will compile the project.
    5. ```> package``` in the sbt console will build a executable jar which is of no use
    6. ```> assembly``` builds a fat jar which could be submitted to the spark cluster 
    7. To run the spark job run the below command from the project root folder 
    ```/Users/satish/Workspace/tools/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --master "spark://local:6066" --deploy-mode cluster --class com.lattice.ReadKinesisStream target/scala-2.11/spark-job.jar```
    8. Check the output on spark master UI.
* Make sure you reload the project if at all sbt misbehaves with ```> reload```