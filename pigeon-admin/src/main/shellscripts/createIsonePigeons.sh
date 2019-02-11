#!/usr/bin/env bash
java -jar pigeonadmin.5.0-jar-with-dependencies.jar  $1 $2 platform $3
java -jar pigeonadmin.5.0-jar-with-dependencies.jar  $1 $2 saasadmin $3
java -jar pigeonadmin.5.0-jar-with-dependencies.jar  $1 $2 saasstat $3
java -jar pigeonadmin.5.0-jar-with-dependencies.jar  $1 $2 session $3
java -jar pigeonadmin.5.0-jar-with-dependencies.jar  $1 $2 appMarket $3
java -jar pigeonadmin.5.0-jar-with-dependencies.jar  $1 $2 publicApps $3
java -jar pigeonadmin.5.0-jar-with-dependencies.jar  $1 $2 blacklist $3
java -jar pigeonadmin.5.0-jar-with-dependencies.jar  $1 $2 shorturl $3
java -jar pigeonadmin.5.0-jar-with-dependencies.jar  $1 $2 logs $3