#!/usr/bin/env bash
export BOOKIE_CONF=/Users/zhengxiangyang/appSdk/bookkeeper-server-4.7.0/conf/bk1.conf
export BOOKIE_LOG_CONF=/Users/zhengxiangyang/appSdk/bookkeeper-server-4.7.0/conf/log4j_1.properties
./bookkeeper shell initnewcluster
./dlog admin bind  -l /pigeon40ledgers -s 127.0.0.1:2181 -c distributedlog://127.0.0.1:2181/pigeon40namespace

