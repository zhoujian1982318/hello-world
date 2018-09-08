package com.examples.spark;

import com.examples.spark.pojo.CollectedWebCacheUsage;
import com.examples.spark.pojo.ContStatsComparator;
import com.examples.spark.pojo.ContentStats;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.*;

public class ContentDemo {

    private static Logger logger = Logger.getLogger(ContentDemo.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws InterruptedException {

        String fileName = "D:\\temp\\data-3.txt";

        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(fileName);

        JavaRDD<CollectedWebCacheUsage> webCaches = lines.map(new Function<String, CollectedWebCacheUsage>() {
            @Override
            public CollectedWebCacheUsage call(String jsonStr) throws Exception {
                CollectedWebCacheUsage webCacheUsage = objectMapper.readValue(jsonStr, CollectedWebCacheUsage.class);
                return webCacheUsage;
            }
        });

        JavaPairRDD<String, ContentStats>  contentLines =  webCaches.flatMapToPair(new PairFlatMapFunction<CollectedWebCacheUsage, String , ContentStats>() {
            @Override
            public Iterator<Tuple2<String , ContentStats>> call(CollectedWebCacheUsage usage) throws Exception {

                List<Tuple2<String , ContentStats>> list = new ArrayList<>();

                for (CollectedWebCacheUsage.WebCacheStat cacheStat : usage.wcStats) {
                    String url = cacheStat.url;
                    if(url==null || "".equals(url)){
                        logger.error("the url must not be null, ignore this item : [" + cacheStat +"]");
                        continue;
                    }
                    String cName = parseWindowContentName(url);
                    ContentStats cSts = new ContentStats(cacheStat.hits, cacheStat.clen);
                    Tuple2<String, ContentStats> record  =   new Tuple2<>(cName, cSts);
                    list.add(record);
                }
                return list.iterator();
            }

            private String parseWindowContentName(String url) {
                int pos = url.lastIndexOf("/");
                String name = "";
                if(pos > 0 ){
                    name = url.substring(pos+1);
                }
                return name;
            }
        });



        JavaPairRDD<String, ContentStats> result = contentLines.reduceByKey(new Function2<ContentStats, ContentStats, ContentStats>() {
            @Override
            public ContentStats call(ContentStats v1, ContentStats v2) throws Exception {
                ContentStats cSts = new ContentStats();
                cSts.setHits(v1.getHits()+ v2.getHits());
                cSts.setSize(v1.getSize() > v2.getSize() ? v1.getSize() : v2.getSize());
                return cSts;
            }
        });

//        List<Tuple2<String,ContentStats>> list  = result.takeOrdered(10,  new Comparator<Tuple2<String, ContentStats>>()  {
//            @Override
//            public int compare(Tuple2<String, ContentStats> o1, Tuple2<String, ContentStats> o2) {
//                return o1._2.getHits() - o2._2.getHits() ;
//            }
//        });

        List<Tuple2<String,ContentStats>> list  = result.takeOrdered(10,  new ContStatsComparator());

        //result.takeOrdered()

        //List<Tuple2<String,ContentStats>> list  = result.take(10);

        for (Tuple2<String,ContentStats> item: list
             ) {
            System.out.println(item._1() + ": " + item._2());
        }


        MINUTES.sleep(10);
    }
}
