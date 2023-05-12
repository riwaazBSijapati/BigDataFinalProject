#!/bin/bash
sudo chmod -R 777 /tmp/hive/  
unzip nba_archive.zip
hdfs dfs -put /home/cloudera/nba_hive_loader/nba_archive /user/cloudera/
rm -rf nba_archive
