#!/usr/bin/env bash

inputNum=$#

# ============== init date info =============
if [ $inputNum -eq 0 ]
then
  bizDate=`date -d '-1 day' +%Y%m%d`
  echo "no specified date, using yesterday ${bizDate}"
else
  echo "use specified date, ${1}"
  bizDate=`date -d $1 +%Y%m%d`
fi

main_test=`hive -e "select count(*)a from tableName where biz_day = ${bizDate};" | grep -v WARN`
main_test=$(echo $main_test | sed  's/^.*_c0.*| \([0-9][0-9]*\).*$/\1/g')

ad_test=`hive -e "select count(*)a from tableName where biz_day = ${bizDate};" | grep -v WARN`
ad_test=$(echo $ad_test | sed  's/^.*_c0.*| \([0-9][0-9]*\).*$/\1/g')

ai_test=`hive -e "select count(*)a from tableName where biz_day = ${bizDate};" | grep -v WARN`
ai_test=$(echo $ai_test | sed  's/^.*_c0.*| \([0-9][0-9]*\).*$/\1/g')

BRAND=test
SERIALID=`date +%Y%m%d%H%M%S%N | cut -b 1-17`
UPDATETIME=`date "+%Y%m%d%H%M%S"`
MASTER=$main_test
MASTER_HIVE=$main_test
MASTERAI=$ai_test
MASTERAI_HIVE=$ai_test
MASTERAD=$ad_test
MASTERAD_HIVE=$ad_test
sed -i 's#\("brand": "\).*#\1'"$BRAND"'",#g' test.json
sed -i 's#\("serialId": "\).*#\1'"$SERIALID"'",#g' test.json
sed -i 's#\("updateTime": "\).*#\1'"$UPDATETIME"'",#g' test.json
sed -i 's#\("master": \).*#\1'$MASTER',#g' test.json
sed -i 's#\("master.hive": \).*#\1'$MASTER_HIVE',#g' test.json
sed -i 's#\("masterAi": \).*#\1'$MASTERAI',#g' test.json
sed -i 's#\("masterAi.hive": \).*#\1'$MASTERAI_HIVE',#g' test.json
sed -i 's#\("masterAd": \).*#\1'$MASTERAD',#g' test.json
sed -i 's#\("masterAd.hive": \).*#\1'$MASTERAD_HIVE'#g' test.json

data=`cat test.json`
echo $data

curl --location \
--request POST 'http://localhost:8083/traffic'  \
--header 'Content-Type: application/json'  \
--data "@test.json"