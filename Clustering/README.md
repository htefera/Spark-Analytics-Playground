# A: Clustering

Task is accomplished using [Scala 2.12.13](https://www.scala-lang.org/api/2.12.13/)
with [sbt](https://www.scala-sbt.org/) used as build tool.

Logic is implemented in [Clustering.scala](src/main/scala/Clustering.scala)
Visualization script is [centroid_viz.py](viz_scripts/centroid_viz.py)

# Usage:

## Prerequisites:

1. Java 8
2. Scala 2.12.13
3. Python3

## Step To Run:

1. To generate centroids run the following command.

```shell
./sbtx run
```

or run the below command to run in docker

```shell
docker run -it --rm -v $(pwd):/app --workdir "/app" --name task_b hseeberger/scala-sbt:11.0.9.1_1.4.6_2.12.12 sbt "run"
```

2. Run the visualization script

```shell
cd viz_scripts
pip install -r requirements.txt
python centroid_viz.py
```

or run on docker

```shell
docker run -it --rm -v $(pwd):/app --workdir "/app" --name task_b python:3.9 bash -c "cd /app/viz_scripts && sh run.sh"
```