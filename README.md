#paraDis

An opensource distributed graph simulation platform.


## Usage


## Configure and Compile
Configure parameters in [config.properties](https://github.com/songqi1990/paraDis/tree/master/src/main/resources/config.properties).

Then build paraDis with maven in project root directory.
```sh
$ mvn package
```
#### Run

Maven packaged 3 seperate runable jar files in ./target directory.

```sh
# launch the coordinator
$ java -Djava.security.policy=security.policy -jar grape-coordinator-0.1.jar
# launch and register worker(s) to the coordinator
$ java -Djava.security.policy=security.policy -jar grape-worker-0.1.jar COORDINATOR_IP
# launch client which sends query to the coordinator
$ java -Djava.security.policy=security.policy -jar grape-client-0.1.jar COORDINATOR_IP
```

## Acknowledgement

- Fast type-specific java collection, FastUtil. http://fastutil.di.unimi.it/
- Graph partitioning lib, Metis. http://glaros.dtc.umn.edu/gkhome/views/metis
- Graph partitioning method, linear deterministic greedy(LDG). [Streaming Graph Partitioning for Large Distributed Graphs](https://www.microsoft.com/en-us/research/wp-content/uploads/2012/08/kdd325-stanton.pdf)
- For details of diversified pattern mining please check our paper [Mining Summaries for Knowledge Graph Search](http://eecs.wsu.edu/~qsong/Files/paper/ICDM2016.pdf)
- For single machine version of this algorithm, pleace check [GraphSum](https://github.com/songqi1990/KnowGraphSum)

## EC2 setup

Add inbound/outbound rules (ALL TCP, ALL ICMP) in EC2 security group 
