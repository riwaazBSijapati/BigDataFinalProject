Hello! This series of packages is designed to load a large NBA dataset into Hive for you to query. 
Please unzip in your VM to the directory: /home/cloudera
run the following 2 commands: 

$chmod u+x loader.sh 
$./loader.sh

Original CSV files will be loaded into this directory:
hdfs://localhost/user/cloudera/nba_archive/csv

Then import HiveLoader2 into Eclipse as a new project.
And run App.java and ViewLoader.java classes.

Tables will be loaded as Hive managed tables, which can be viewed easily
in Beeline shell:

$ beeline

!connect jdbc:hive2://localhost:10000 cloudera cloudera 
