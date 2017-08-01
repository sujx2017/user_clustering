# user_clustering
#! /bin/bash
first=$1
second=$2

while [[ "$first" != "$second" ]]; do
echo $first
year=`expr substr ${first} 1 4`
month=`expr substr ${first} 5 2`
day=`expr substr ${first} 7 2`


hive -e"
add jar /data/sujx/UDF_qiujx.jar;
CREATE TEMPORARY FUNCTION num_array as 'cn.qiujx.hive.udf.NumberBetweenTwoInteger';

set  hive.exec.dynamic.partition=true; 
set  hive.exec.dynamic.partition.mode=nonstrict;
set  hive.exec.max.dynamic.partitions=1000;
set  hive.exec.max.dynamic.partitions.pernode=1000;
set  hive.exec.max.created.files=655350;


insert into table sujx.day_hour_login_num
partition(year,month,day)
select
    aa.userid,
    aa.hour,
    count(aa.hour) as num,
    aa.year,
    aa.month,
    aa.day
from
    (select
        a.userid,
        hour,
        a.year,
        a.month,
        a.day
    from
        (select
            userid,
            num_array(from_unixtime(logindt-8*3600,'HH'),if(from_unixtime(logindt-8*3600,'HH')>from_unixtime(logoutdt-8*3600,'HH'),23,from_unixtime(logoutdt-8*3600,'HH'))) as hour_array,
            from_unixtime(logindt-8*3600,'yyyy') as year,
            from_unixtime(logindt-8*3600,'MM') as month,
            from_unixtime(logindt-8*3600,'dd') as day
        from
            cdd.cdd_shuffle_useronline
        where
            concat(year,month,day) = '$first'
        union all
        select
            userid,
            case when from_unixtime(logindt-8*3600,'HH')>from_unixtime(logoutdt-8*3600,'HH') then num_array(0,from_unixtime(logoutdt-8*3600,'HH')) end as hour_array,
            from_unixtime(logoutdt-8*3600,'yyyy') as year,
            from_unixtime(logoutdt-8*3600,'MM') as month,
            from_unixtime(logoutdt-8*3600,'dd') as day
        from
            cdd.cdd_shuffle_useronline
        where
            concat(year,month,day) = '$first'
        )a
    LATERAL VIEW explode(a.hour_array) asTable as hour
    )aa
group by
    aa.userid,aa.hour,aa.year,aa.month,aa.day;"


let first=`date -d "-1 days ago ${first}" +%Y%m%d`

done
if [ $? -ne 0 ];then
      exit 1
fi
