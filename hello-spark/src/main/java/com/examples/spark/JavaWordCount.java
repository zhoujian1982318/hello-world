package com.examples.spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public final class JavaWordCount {

    private static Logger logger = Logger.getLogger(JavaWordCount.class);

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        String fileName = "file:\\E:\\sql\\sql-problem.txt";

//        SparkSession spark = SparkSession
//                .builder()
//                .appName("JavaWordCount")
//                .getOrCreate();

//        SparkSession spark = SparkSession
//                .builder()
//                .master("local[2]")
//                .appName("JavaWordCount")
//                .getOrCreate();

//        spark.read().text()

//        JavaRDD<String> lines = spark.read().textFile(fileName).javaRDD();

        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]");

        conf.set("spark.local.dir", "E:\\temp\\spark\\");

        JavaSparkContext sc = new JavaSparkContext(conf);

       // sc.defaultParallelism()

        JavaRDD<String> lines = sc.textFile(fileName);



        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                });


        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        long number  = counts.count();

        logger.info("the number is " + number);

        TimeUnit.MINUTES.sleep(10);
        sc.stop();
    }
}
