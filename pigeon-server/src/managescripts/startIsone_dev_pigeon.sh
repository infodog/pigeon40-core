#!/usr/bin/env bash
nohup java -Dlog4j.configuration=file:../configs/log4j.properties -DNodeName=server1 -jar ../lib/pigeonserver.4.0-SNAPSHOT-jar-with-dependencies.jar ../configs/pigeonServerIsoneV45_develop.json 8879 50 1>../logs/pigeonserver.out 2>../logs/pigeonserver.err &