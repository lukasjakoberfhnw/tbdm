# 

# Description

This project was done as part of a module at the University of Camerino. It includes several Scala scripts that try to approximate the correct clustering of low-level events into a higher-order abstraction. The main focus was on using Spark, especially the GraphX environment, to try to calculate or approximate clusters.

# Installation

```bash
# Update and Upgrade Ubuntu/Linux Distro
sudo apt update
sudo apt upgrade

# Install required packages
sudo apt install -y wget curl git openjdk-11-jdk scala

# Get Spark
wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz

tar -xvf spark-3.5.5-bin-hadoop3.tgz
sudo mkdir /opt/spark
sudo mv spark-3.5.5-bin-hadoop3/* /opt/spark

nano ~/.bashrc

# Add the following lines to the end of the file
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export SPARK_HOME=/opt/spark
export HADOOP_HOME=/opt/hadoop/hadoop-3.4.1
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# setup for sbt
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | sudo apt-key add

# Update apt
sudo apt update

# Install sbt
sudo apt install sbt

```

# Usage

Use the following command to start sbt and select a main class of a script to run the file.

```
sbt run
```

You will be promted to select a main class from one of the files. Each algorithm has a A[1-12] prefix. This indicates that it will return a .txt file into the working directory with the resulting clusters. Feel free to run them and inspect the resulting .txt file.

## Performance Metrics

The performance of the clusters are ranked by a custom metric. First, we check how many relationships are found that also exist in the ground truth dataset. Then, we calculate the number of relationships found with respect to all possible relationships between nodes. A1 retrieves the best results based on the current state of the project.

# Future Work

This repository could be improved by investigating further approaches to use clustering algorithms combined with time. We mainly focused on the sequentiality of data and how they are linked. The usage of graphs are interesting, however, the GraphX library only really supports Scala as a programming language. Therefore, it is very limited as majority of developers within the statistical and machine learning environment use Python as their main language. 

It could be argued if the cluster calculations should be done using a big data technology or solely the transformation from the sequential data into an aggregated form and use Python for the final clusters. As we have used a directly-follows-graph (DFG) within our algorithm, the data is reduced enough to compute the clusters on a single machine. 

# Support

If you have any questions regarding the dataset or the approaches, please feel free to reach out to one of the authors of the repository listed below.

arbnor.bekiri@students.fhnw.ch \
samuel.hilty@students.fhnw.ch \
lukas.jakober@students.fhnw.ch
