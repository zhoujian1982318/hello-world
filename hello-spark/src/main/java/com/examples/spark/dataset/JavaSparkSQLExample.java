package com.examples.spark.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.col;

public class JavaSparkSQLExample {

    public static void main(String[] args) {
        // $example on:init_session$
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[2]")
                .config("spark.sql.warehouse.dir", "file:///E:/temp/warehouse")
                .config("spark.local.dir", "E:\\temp\\spark")
                .getOrCreate();
        // $example off:init_session$

        runBasicDataFrameExample(spark);
        //runDatasetCreationExample(spark);
        //runInferSchemaExample(spark);
        //runProgrammaticSchemaExample(spark);
        try {
            TimeUnit.MINUTES.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        spark.stop();
    }

    private static void runBasicDataFrameExample(SparkSession spark) {
        // $example on:create_df$
        Dataset<Row> df = spark.read().json("../hello-spark/src/main/resources/people.json");

        // Displays the content of the DataFrame to stdout
        //df.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+
        // $example off:create_df$

        // $example on:untyped_ops$
        // Print the schema in a tree format
       // df.printSchema();
        // root
        // |-- age: long (nullable = true)
        // |-- name: string (nullable = true)

        // Select only the "name" column

        //df.select("name").show();
        // +-------+
        // |   name|
        // +-------+
        // |Michael|
        // |   Andy|
        // | Justin|
        // +-------+

        // Select everybody, but increment the age by 1
       // df.select(col("name"), col("age").plus(1)).show();
        // +-------+---------+
        // |   name|(age + 1)|
        // +-------+---------+
        // |Michael|     null|
        // |   Andy|       31|
        // | Justin|       20|
        // +-------+---------+

        // Select people older than 21
       // df.filter(col("age").gt(21)).show();
        // +---+----+
        // |age|name|
        // +---+----+
        // | 30|Andy|
        // +---+----+

        // Count people by age
        //df.groupBy("age").count().show();
        // +----+-----+
        // | age|count|
        // +----+-----+
        // |  19|    1|
        // |null|    1|
        // |  30|    1|
        // +----+-----+
        // $example off:untyped_ops$

        // $example on:run_sql$
        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+
        // $example off:run_sql$
    }
}
