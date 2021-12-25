# C: Recommendation Systems

Task is accomplished using [Scala 2.12.13](https://www.scala-lang.org/api/2.12.13/)
with [sbt](https://www.scala-sbt.org/) used as build tool.

[BaselinePredictor.scala](src/main/scala/BaselinePredictor.scala) implements a baseline predictor during the lecture.
[Recommender.scala](src/main/scala/Recommender.scala) implements exploration of datasets, evaluates Baseline and ALS
model, and we choose model with lower Root Mean Square Error and use it to recommend top movies to all users.

# Usage:

## Prerequisites:

1. Java 8
2. Scala 2.12.13

## Step To Run:

1. To run the program local.

```shell
./sbtx run
```

or run the below command to run in docker

```shell
docker run -it --rm -v $(pwd):/app --workdir "/app" --name task_c hseeberger/scala-sbt:11.0.9.1_1.4.6_2.12.12 sbt "run"
```