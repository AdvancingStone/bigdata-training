#zkStart.sh
#start zk cluster
for host in master slave1 slave2
do
	echo "===========start zk cluster :$host==============="
	ssh  $host 'source .bash_profile; /opt/software/zookeeper/bin/zkServer.sh start /opt/software/zookeeper/conf/zoo.cfg '
done
sleep 3s
#check status
for host in master slave1 slave2
do
	echo "===========checking zk node status :$host==============="
	ssh  $host 'source .bash_profile; /opt/software/zookeeper/bin/zkServer.sh status'
done


#zkStop.sh
#stop zk cluster
for host in master slave1 slave2
do
	echo "===========$host==============="
	ssh  $host '.bash_profile; /opt/software/zookeeper/bin/zkServer.sh stop'
done