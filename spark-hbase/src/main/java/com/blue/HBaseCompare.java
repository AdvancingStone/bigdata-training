package com.blue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class HBaseCompare {
    private  Configuration getKerberosLoginConf(){
        Configuration hconf = HBaseConfiguration.create();
        hconf.set("hbase.rpc.timeout", "10000");
        hconf.set("hbase.client.retries.number", "2");
        hconf.set("hbase.client.operation.timeout", "10000");
        InputStream stream1 = this.getClass().getClassLoader().getResourceAsStream("local_hbase/core-site.xml");
        InputStream stream2 = this.getClass().getClassLoader().getResourceAsStream("local_hbase/hbase-site.xml");
        InputStream stream3 = this.getClass().getClassLoader().getResourceAsStream("local_hbase/hdfs-site.xml");
        hconf.addResource(stream1);
        hconf.addResource(stream2);
        hconf.addResource(stream3);
        return hconf;
    }

    //获取hbase数据并转换

    private static JavaPairRDD<String, Map<String,String>> getTableDataRDD(Configuration hconf,String tableName, JavaSparkContext sc) throws IOException {
        hconf.set(TableInputFormat.INPUT_TABLE,tableName);
        //添加scan
        String scanToString = TableMapReduceUtil.convertScanToString(new Scan());
        hconf.set(TableInputFormat.SCAN, scanToString);
        //hbase数据转化为RDD

        JavaPairRDD<ImmutableBytesWritable, Result> dataRDD = sc.newAPIHadoopRDD(hconf, TableInputFormat.class,ImmutableBytesWritable.class,Result.class);
        //hbase的Result对象不支持序列化
        JavaPairRDD<String, Map<String,String>> dataRowsRDD = dataRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Map<String,String>>() {
            @Override
            public Tuple2<String, Map<String,String>> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                Result result =  immutableBytesWritableResultTuple2._2;
                HashMap<String,String> resultMap = new HashMap<String, String>();
                for(Cell cell : result.rawCells()) {
                    resultMap.put(new String(CellUtil.cloneQualifier(cell)).toLowerCase(), new String(CellUtil.cloneValue(cell)));
                }
                return new Tuple2<>(Bytes.toString(result.getRow()),resultMap);
            }
        });
        return dataRowsRDD;
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("hbase example").setMaster("local");

        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        SparkContext sparkContext = session.sparkContext();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkContext);


        HBaseCompare hBaseCompare = new HBaseCompare();
        Configuration srcConf = hBaseCompare.getKerberosLoginConf();

        try{
            JavaPairRDD<String, Map<String,String>> srcRowsRDD = getTableDataRDD(srcConf,"u_analysis:purchase_send_coupon_new6_hbase",sc);
            long count = srcRowsRDD.count();
            Tuple2<String, Map<String, String>> first = srcRowsRDD.rdd().first();
            System.out.println(first);

            System.out.println("count====="+count);
            //使用数据
            //...
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
