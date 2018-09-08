package com.examples.spark;

import com.examples.spark.pojo.ApInfo;
import com.examples.spark.pojo.ClientCntInfo;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JoinExample {

    private static Logger logger = Logger.getLogger(HdfsExample.class);

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("join example").setMaster("local[2]");

        conf.set("spark.local.dir", "E:\\temp\\spark\\");
//
//        conf.set("spark.shuffle.compress", "false");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> apInfo = sc.textFile("hdfs://wentong-01.eng.hz.relay2.cn:9000/mgmt/ap/ap.txt");

        //JavaRDD<String> acctInfo = sc.textFile("hdfs://wentong-01.eng.hz.relay2.cn:9000/mgmt/account/account.txt");

        JavaRDD<String> allCltInfo = sc.textFile("hdfs://wentong-01.eng.hz.relay2.cn:9000/mgmt/cs/client-station.txt");

        logger.info("start join");

        JavaPairRDD<String, ClientCntInfo> clientCntRdd = allCltInfo.mapPartitionsToPair((lines)->{
            List<Tuple2<String, ClientCntInfo>> list = new ArrayList<>();
            while(lines.hasNext()){
                String line = lines.next();
                String[] strArrays = line.split(",");
                String clientMac = strArrays[0];
                String apMac = strArrays[1];
                int status = Integer.valueOf(strArrays[5]);
                ClientCntInfo cltCntInfo = new ClientCntInfo();
                cltCntInfo.setApMac(apMac);
                cltCntInfo.setClientMac(clientMac);
                cltCntInfo.setStatus(status);
                list.add(new Tuple2<>(apMac, cltCntInfo));
            }
            return list.iterator();
        });


        JavaPairRDD<String,ApInfo> apPairRdd =  apInfo.mapToPair((line)->{
            String []  strArr = line.split(",");
            long acctId = Long.valueOf(strArr[0]);
            String apMac = strArr[1];
            String apName = strArr[2];
            ApInfo ap = new ApInfo();
            ap.setApMac(apMac);
            ap.setApName(apName);
            ap.setAcctId(acctId);
           return new Tuple2<>(apMac, ap);
        });

//        JavaPairRDD<Long, String> acctPairRdd =  acctInfo.mapToPair((line)->{
//            String []  strArr = line.split(",");
//            long acctId = Long.valueOf(strArr[0]);
//            String acctName = strArr[1];
//
//            return  new Tuple2<>(acctId, acctName);
//        });

//        JavaPairRDD<Long, Tuple2<ApInfo, String>> apAcctInfo =  apPairRdd.join(acctPairRdd, 1);
//
//        JavaRDD<ApInfo> apJoinInfo = apAcctInfo.map((r)->{
//            Tuple2<ApInfo,String> item  = r._2;
//            item._1.setAcctName(item._2);
//            return item._1;
//        });



//        JavaPairRDD<String, Tuple2<ApInfo, ClientCntInfo>> apAcctInfo =  apPairRdd.join(clientCntRdd, 10);
//
//        JavaRDD<ClientCntInfo> cltCntApJoinRdd = apAcctInfo.map((t)->{
//            ClientCntInfo info =  t._2._2;
//            info.setApName(t._2._1.getApName());
//            return info;
//        });

        //不管大表放右边， 还是小表放右边。速度差不多,  估计都是用. sort merge join. 都要按key 进行排序。 内存不够都要spill 数据到磁盘.
        JavaRDD<ClientCntInfo> cltCntApJoinRdd  = clientCntRdd.join(apPairRdd, 10).map((t)->{
            ClientCntInfo info = t._2._1;
            info.setApName(t._2._2.getApName());
            return info;
        });

        //Shuffle Spill (Memory) ?  Shuffle spill (disk) ??
        cltCntApJoinRdd.saveAsTextFile("hdfs://wentong-01.eng.hz.relay2.cn:9000/mgmt/cs/output/spark/clientCnt-join-ap");

//        long count  = cltCntApJoinRdd.count();
//
//        logger.info("the finally count is " + count);

        logger.info("end join");

        //TimeUnit.MINUTES.sleep(15);

        sc.stop();
    }
}
