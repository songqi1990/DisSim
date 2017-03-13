#!/bin/bash

KEY_FILE_NAME = "amazon-grape-server.pem"
GRAPH_FILE_NAME = "amazon.dat"
GRAPH_PARTITION_COUNT = 3

#ssh login
ssh

#apt-get update
sudo apt-get update

#if no git
sudo apt-get install git

#config ssh between machines without password
git clone https://github.com/songqi1990/deploy.git
cp deploy/$KEY_FILE_NAME ~/.ssh/$KEY_FILE_NAME
chmod 400 ~/.ssh/$KEY_FILE_NAME 400

sudo vim /etc/ssh/ssh_config
IdentityFile ~/.ssh/amazon-grape-server.pem

#if no java
sudo apt-get install openjdk-7-jdk

#set java home
#get JAVA_HOME: which java
export JAVA_HOME=/usr

#get maven
sudo apt-get install maven

#get grape repo
git clone https://github.com/yecol/grape.git
cd grape

#---------------COORDINATOR ONLY-------------------
##for cmake
#if no gcc
sudo apt-get install build-essential
#if no cmake
sudo apt-get install cmake

#make metis
cd lib/metis-5.1.0
make config
make
cd ../../
#--------------------------------------------------

#compile
mvn package

#---------------COORDINATOR ONLY-------------------
#partition graph
./target/gpartition $GRAPH_FILE_NAME $GRAPH_PARTITION_COUNT
#distributed partitions
for machineName in arr
	scp -i ~/.ssh/$KEY_FILE_NAME ./target/$GRAPH_FILE_NAME* 172.31.50.121:~/grape/target/
done

COORDINATOR_HOST = 172.31.50.120

#begin work
java -Djava.security.policy=./target/security.policy -jar ./target/grape-coordinator-0.1.jar 
java -Djava.security.policy=./target/security.policy -jar ./target/grape-worker-0.1.jar 172.31.50.120
java -Djava.security.policy=./target/security.policy -jar ./target/grape-client-0.1.jar 172.31.50.120

#begin localhost
java -Xmx1000M -Djava.security.policy=./target/security.policy -jar ./target/grape-coordinator-0.1.jar 
java -Xmx3000M -Djava.security.policy=./target/security.policy -jar ./target/grape-worker-0.1.jar localhost
java -Djava.security.policy=./target/security.policy -jar ./target/grape-client-0.1.jar localhost


