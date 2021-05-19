hdfs_path=/user/hive
hdfs dfs -du $hdfs_path | cat | while read line
do
  arr=($line)
  if (( ${arr[0]} == 0 ))
  then
    echo $line
  fi
done