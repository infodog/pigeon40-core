#!/usr/bin/env bash
nohup java -Dlog4j.configuration=file:../configs/log4j.properties -DNodeName=shard0_s1 -jar ../lib/pigeonserver.5.0-SNAPSHOT-jar-with-dependencies.jar ../configs/pigeonserver_shard0_s1.json 8879 50 1>../logs/pigeonserver_shard0_s1.out 2>../logs/pigeonserver_shard0_s1.err &
nohup java -Dlog4j.configuration=file:../configs/log4j.properties -DNodeName=shard1_s1 -jar ../lib/pigeonserver.5.0-SNAPSHOT-jar-with-dependencies.jar ../configs/pigeonserver_shard1_s1.json 8878 50 1>../logs/pigeonserver_shard1_s1.out 2>../logs/pigeonserver_shard1_s1.err &
nohup java -Dlog4j.configuration=file:../configs/log4j.properties -DNodeName=shard0_s2 -jar ../lib/pigeonserver.5.0-SNAPSHOT-jar-with-dependencies.jar ../configs/pigeonserver_shard0_s2.json 8877 50 1>../logs/pigeonserver_shard0_s2.out 2>../logs/pigeonserver_shard0_s2.err &
nohup java -Dlog4j.configuration=file:../configs/log4j.properties -DNodeName=shard1_s2 -jar ../lib/pigeonserver.5.0-SNAPSHOT-jar-with-dependencies.jar ../configs/pigeonserver_shard1_s2.json 8876 50 1>../logs/pigeonserver_shard1_s2.out 2>../logs/pigeonserver_shard1_s2.err &