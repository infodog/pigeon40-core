#!/usr/bin/env bash
export root=`pwd`/..
cd ${root}/apps/kafka_2.11-2.1.0/bin
sh kafka-server-start.sh ${root}/configs/kafka.server.properties