package com.examples.spark;

import com.examples.spark.pojo.ClientCntCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class HdfsExample {

    private static Logger logger = Logger.getLogger(HdfsExample.class);

    public static void main(String[] args) throws InterruptedException {
//        SparkSession spark = SparkSession
//                .builder()
//                .master("local[2]")
//                .appName("hdfs example")
//                .getOrCreate();

       // spark.read().orc()

        //spark.read().
//
//       SparkContext sparkContext = spark.sparkContext();
//
//       SparkConf conf  = sparkContext.conf();
//
//       JavaSparkContext sc = new JavaSparkContext(conf);
//        readHdfsFileByNewAPIHadoopFile();
//      JavaRDD<String> lines = sc.textFile("hdfs://wentong-01.eng.hz.relay2.cn:9000/mgmt/account/account.txt");

//      JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//         @Override
//         public Iterator<String> call(String s) {
//             return Arrays.asList(s.split(",")).iterator();
//          }
//      });


    }

    private static void readHdfsFileByNewAPIHadoopFile() {

        SparkConf conf = new SparkConf().setAppName("hdfs example").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //JavaPairRDD<LongWritable, Text> lines =  sc.hadoopFile("hdfs://wentong-01.eng.hz.relay2.cn:9000/mgmt/account/account.txt", TextInputFormat.class, LongWritable.class, Text.class);
        Configuration hadoopconf = new Configuration();
        JavaRDD<String> lines = sc.newAPIHadoopFile("/mgmt/ap/ap.txt",
                TextInputFormat.class, LongWritable.class, Text.class, hadoopconf).map((r)->{
            String test = r._2.toString();
            return test;
        });
        List<String> list = lines.top(10);

        for (String line : list) {
            logger.info("read the record :" + line);
        }
        sc.stop();
    }
}
