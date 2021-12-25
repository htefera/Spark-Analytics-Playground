# D: Spatial Data Analysis

Task is accomplished using [Scala 2.12.13](https://www.scala-lang.org/api/2.12.13/)
with [sbt](https://www.scala-sbt.org/) used as build tool.

[SpatialWithIndex.scala](src/main/scala/SpatialWithIndex.scala) implements driver which evaluates for 

# Usage:

## Prerequisites:

1. Java 8
2. Scala 2.12.13

## Step To Run:

1. To generate centroids run the following command.

```shell
./sbtx run
```

or run the below command to run in docker

```shell
docker run -it --rm -v $(pwd):/app --workdir "/app" --name task_c hseeberger/scala-sbt:11.0.9.1_1.4.6_2.12.12 sbt "run"
```