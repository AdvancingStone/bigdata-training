for host in master slave1 slave2
do
	echo "===========checking clickhouse server node status :$host==============="
	ssh  $host 'source /etc/profile; service clickhouse-server status'
done
