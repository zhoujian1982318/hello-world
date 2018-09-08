package com.examples.spark;

import com.examples.spark.pojo.ClientCntCount;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CntCltCountGroupByAccount {

    private static int number = 0;

    public static void main(String[] args) {

        testClientCntCountExample();

        //testClientCountExample();
    }

    private static void testClientCountExample() {

        SparkConf conf = new SparkConf().setAppName("client connect count example").setMaster("local[2]");
        conf.set("spark.local.dir", "E:\\temp\\spark\\");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> allCltInfo = sc.textFile("hdfs://wentong-01.eng.hz.relay2.cn:9000/mgmt/cs/client-station.txt");
        //allCltInfo.persist(StorageLevel.MEMORY_AND_DISK_SER());
//        JavaRDD<ClientCntCount> acctCltCntInfo = allCltInfo.map((line)->{
//            String[] strArrays = line.split(",");
//            String clientMac = strArrays[0];
//            Long acctId  = Long.valueOf(strArrays[4]);
//            return new ClientCntCount(clientMac, acctId,0);
//        }).sortBy((c)-> {
//            return c.getCount();
//        }, false,30);

        JavaPairRDD<String, Long> pairRdd = allCltInfo.map((line)->{
            String[] strArrays = line.split(",");
            String clientMac = strArrays[0];
            Long acctId  = Long.valueOf(strArrays[4]);
            return new ClientCntCount(clientMac, acctId,0);
        }).mapToPair((t)->{
            String clientMac = t.getClientMac();
            return new Tuple2<>(clientMac, t.getAcctId());
        }).sortByKey();

//        JavaRDD<ClientCntCount> acctCltCntInfo = allCltInfo.mapToPair((line)->{
//            String[] strArrays = line.split(",");
//            String clientMac = strArrays[0];
//            return new Tuple2<>(clientMac, 1);
//        }).reduceByKey((c1, c2)->{
//            return c1 +c2;
//        },2).map((t)->{
//            return new ClientCntCount(t._1, t._2);
//        }).sortBy((c)-> {
//            return c.getCount();
//        }, false, 2);



        pairRdd.saveAsTextFile("hdfs://wentong-01.eng.hz.relay2.cn:9000/mgmt/cs/output/spark/client-desc");
        //long number = pairRdd.count();
        System.out.println("the number is "+ number);
        try {
            TimeUnit.MINUTES.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    private static void testClientCntCountExample() {

        SparkConf conf = new SparkConf().setAppName("client connect count example").setMaster("local[2]");
        conf.set("spark.local.dir", "E:\\temp\\spark\\");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> allCltInfo = sc.textFile("hdfs://wentong-01.eng.hz.relay2.cn:9000/mgmt/cs/client-station.txt");

        //JavaPairRDD<Long, Integer> acctCltCntInfo =
        JavaRDD<ClientCntCount> acctCltCntInfo =   allCltInfo.filter((line)->{
            String[] strArrays = line.split(",");
            Integer status = Integer.valueOf(strArrays[5]);
            if(status==3){
                return true;
            }else{
                return false;
            }
        }).mapToPair((line)->{
            String[] strArrays = line.split(",");
            Long acctId  = Long.valueOf(strArrays[4]);
            return new Tuple2<>(acctId, 1);
        }).reduceByKey((c1, c2)->{
            return c1+c2;
        }, 2).map((t)->{
            return new ClientCntCount(t._1, t._2);
        });
//                .sortBy((c)->{
//            return c.getCount();
//        }, false,2);


        acctCltCntInfo.saveAsTextFile("hdfs://wentong-01.eng.hz.relay2.cn:9000/mgmt/cs/output/spark/client-connect");

        try {
            TimeUnit.MINUTES.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        sc.stop();
    }
}
