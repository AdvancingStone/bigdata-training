for host in master slave1 slave2
do
	echo "===========stop clickhouse server cluster :$host==============="
	ssh  $host 'source /etc/profile; service clickhouse-server stop '
done
