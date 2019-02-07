#!/usr/bin/env bash
export root=`pwd`/..
cd ${root}/apps/zookeeper-3.4.13/bin
sh zkServer.sh start ${root}/configs/zoo.cfg