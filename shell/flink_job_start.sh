export HADOOP_CONF_DIR=/etc/hadoop/conf
export HADOOP_CLASSPATH=`hadoop classpath`

export FLINK_HOME=/opt/software/flink-1.9.1
export WORK_HOME=/data/crash_test
# -s hdfs:/user/view/crash_test \
$FLINK_HOME/bin/flink run \
 -p 2 \
 -m yarn-cluster\
 -yjm 1024m\
 -ytm 2048m\
 -ynm crash-test \
 -ys 2 \
 -c com.bluehonour.flink.crash_test \
 $WORK_HOME/crash_test.jar