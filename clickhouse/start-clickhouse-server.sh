for host in master slave1 slave2
do
	echo "===========start clickhouse server cluster :$host==============="
	ssh  $host 'source /etc/profile;  clickhouse-server --config-file=/etc/clickhouse-server/config.xml '
done
sleep 3s
#check status
for host in master slave1 slave2
do
	echo "===========checking clickhouse server node status :$host==============="
	ssh  $host 'source /etc/profile; service clickhouse-server status'
done
