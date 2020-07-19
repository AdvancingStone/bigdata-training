#kafka-server-start.sh
#start kafka cluster
for host in master slave1 slave2
do
	echo "===========start kafka cluster :$host==============="
	ssh  $host 'source .bash_profile; /opt/software/kafka/bin/kafka-server-start.sh -daemon /opt/software/kafka/config/server.properties '
done


#kafka-server-stop.sh
#stop kafka cluster
for host in master slave1 slave2
do
	echo "===========stop kafka cluster :$host==============="
	ssh  $host 'source .bash_profile; /opt/software/kafka/bin/kafka-server-stop.sh'
done
