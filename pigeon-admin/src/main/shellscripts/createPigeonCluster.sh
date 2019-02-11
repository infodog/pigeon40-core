#!/usr/bin/env bash
# $1 --- zooconnectstring, eg. localhost:2181
# $2 --- configfile
# $3 --- podname
# $4 --- "kafkaBootstrapServers" eg. "localhost:9092",
java -jar pigeonadmin.5.0-jar-with-dependencies.jar $1 $2 $3 $4