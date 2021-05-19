hdfspath=/user/hive/warehouse/test/dt=20210330
hdfs dfs -ls $hdfspath | cat | while read line
do
  echo $line
#  arr=($line)
#  echo ${arr[0]} ${arr[1]} ${arr[2]} ${arr[3]} ${arr[4]} ${arr[5]} ${arr[6]} ${arr[7]}
  privilege=${arr[0]}
  fileOrDir=${privilege:0:1}
  user=${privilege:1:3}
  group=${privilege:4:3}
  other=${privilege:7:3}
#  echo $fileOrDir $user $group $other

  if [ $group != "rwx" ]; then
    echo $line
  fi
done
