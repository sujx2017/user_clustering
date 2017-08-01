# user_clustering
create table sujx.day_hour_online
(userid int,
 hour string
)
partitioned by (year string,month string,day string)
row format delimited
fields terminated by ','
stored as textfile;



add jar /data/sujx/UDF_qiujx.jar;
CREATE TEMPORARY FUNCTION num_array as 'cn.qiujx.hive.udf.NumberBetweenTwoInteger';

set  hive.exec.dynamic.partition=true; 
set  hive.exec.dynamic.partition.mode=nonstrict;
set  hive.exec.max.dynamic.partitions=1000;
set  hive.exec.max.dynamic.partitions.pernode=1000;
set  hive.exec.max.created.files=655350;

insert overwrite table sujx.day_hour_online
partition(year,month,day)
select
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
        concat(year,month,day) = '20170101'
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
        concat(year,month,day) = '20170101'
    )a
LATERAL VIEW explode(a.hour_array) asTable as hour
group by
    a.userid,hour,a.year,a.month,a.day;
