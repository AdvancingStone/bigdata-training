#!/bin/bash
#now_timestamp=$(($(date +%s%N)/1000000))
now_timestamp=`date +%s%3N`
if [ -f "./test" ];
then
	pre1h_timestamp=`tail test`
else 
	pre1h_timestamp=$(expr $now_timestamp - 3600000)
fi
yesterday=`date -d '-1 day' +%Y%m%d`
today=`date +%Y%m%d`
serialId=`date +%Y%m%d%H%M%S%N | cut -b 1-17`
updateTime=`date "+%Y%m%d%H%M%S"`

echo $now_timestamp
echo $pre1h_timestamp
echo $yesterday
echo $today
echo $serialId
echo $updateTime

cat >test << EOF
$now_timestamp
EOF

traffic_test_result=`hive -e "select adid, count(1) as total from (select get_json_object(event_params, '$.adid') as adid from  schema.tablename where biz_day between ${yesterday} and ${today} and event_timestamp>${pre1h_timestamp} and event_timestamp<=${now_timestamp})tmp group by adid;" | grep -v WARN`

echo -e "====================traffic_test_result is: =================\n${traffic_test_result}"
arr=($traffic_test_result)
echo -e $arr
arr_len=${#arr[@]}
length=`expr $arr_len / 2`
echo -e "arr_len is $arr_len\n${arr[@]}"

adId=()
total=()

for ((i=0;i<$arr_len;i++))
do
	if [ `expr $i % 2` -eq 0 ];
	then
		adId=("${adId[@]}" ${arr[$i]})
	else
		total=("${total[@]}" ${arr[$i]})
	fi
done
echo ${adId[@]}
echo ${total[@]}



printf "{\n"
printf "\t\"brand\": \"test\",\n"
printf "\t\"client\": \"test\",\n"
printf "\t\"serialId\": \"${serialId}\",\n"
printf "\t\"updateTime\": \"${updateTime}\",\n"
printf "\t\"data\": [\n"
for ((i=0;i<$length;i++))
do
	if [ "$i" == $(expr $length - 1) ];
	then
		printf "\t\t{\n\t\t\t\"adId\": \"${adId[$i]}\",\n"
		printf "\t\t\t\"total\": ${total[$i]}\n"
		printf "\t\t}\n"
	else
		printf "\t\t{\n\t\t\t\"adId\": \"${adId[$i]}\",\n"
		printf "\t\t\t\"total\": ${total[$i]}\n"
		printf "\t\t},\n"
	fi
done
printf "\t]\n"
printf "}\n"


data=`
for ((i=0;i<$length;i++))
do
	if [ "$i" == $(expr $length - 1) ];
	then
		printf "\t\t{\n\t\t\t\"adId\": \"${adId[$i]}\",\n"
		printf "\t\t\t\"total\": ${total[$i]}\n"
		printf "\t\t}\n"
	else
		printf "\t\t{\n\t\t\t\"adId\": \"${adId[$i]}\",\n"
		printf "\t\t\t\"total\": ${total[$i]}\n"
		printf "\t\t},\n"
	fi
done
`

cat > test.json <<EOF
{
	"brand": "test",
	"client": "test",
	"serialId": "${serialId}",
	"updateTime": "${updateTime}",
	"data":[
${data}
	]
}
EOF


user_data=`cat test.json`
echo -e "==========read test.json data:\n$user_data"
cat >> ../log/test.log <<EOF
======================================================================================================================================================
now_timestamp is $now_timestamp
pre1h_timestamp is $pre1h_timestamp
${user_data}
EOF

curl --location \
--request POST 'http://ip/'  \
--header 'Content-Type: application/json'  \
--data "@test.json"