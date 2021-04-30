修改MySQL的hive schema

（1）修改表字段注解和表注解

```mysql
alter table COLUMNS_V2 modify column COMMENT varchar(256) character set utf8;
alter table TABLE_PARAMS modify column PARAM_VALUE varchar(4000) character set utf8;
```

（2）修改分区字段注解

```mysql
alter table PARTITION_PARAMS modify column PARAM_VALUE varchar(4000) character set utf8 ;
alter table PARTITION_KEYS modify column PKEY_COMMENT varchar(4000) character set utf8;
```

（3）修改索引注解

```mysql
alter table INDEX_PARAMS modify column PARAM_VALUE varchar(4000) character set utf8;
```


