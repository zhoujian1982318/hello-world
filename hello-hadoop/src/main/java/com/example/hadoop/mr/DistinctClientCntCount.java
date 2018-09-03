package com.example.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class DistinctClientCntCount {

    private static Logger logger = Logger.getLogger(DistinctClientCntCount.class);

    public static class DistinctClientMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private Text content = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String txt = value.toString();
            String[] spliturl = txt.split(",");
            String clientMac = spliturl[0];
            if(clientMac!=null || "".equals(clientMac)){
                content.set(clientMac);
                context.write(content, NullWritable.get());
            }
        }
    }

    public static class DistinctClientReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        private long count = 0L;
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            //context.write(key, NullWritable.get());
            count++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text countText = new Text(String.valueOf(count));
            context.write(countText, NullWritable.get());
        }
    }

    public static class ClientCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text countKey = new Text("count");
        private final static IntWritable one = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(countKey, one);
        }
    }

    public static class ClientCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static boolean deleteFile(Configuration conf, String remoteFilePath, boolean recursive) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        boolean result = fs.delete(new Path(remoteFilePath), recursive);
        fs.close();
        return result;
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String inputPath = "/mgmt/cs/client-station.txt";
        String distinctOutputPath = "/mgmt/cs/output/distinct/";

        String countOutputPath = "/mgmt/cs/output/count/";

        Configuration conf = new Configuration();
        conf.set("mapreduce.job.jar", "C:\\Users\\Administrator\\git\\hello-world\\hello-hadoop\\target\\hello-hadoop-1.0.jar");

        DistinctClientCntCount.deleteFile(conf, distinctOutputPath, true);
        DistinctClientCntCount.deleteFile(conf, countOutputPath, true);

        Job job = Job.getInstance(conf, "distinct client job");
        job.setJarByClass(DistinctClientCntCount.class);
        job.setMapperClass(DistinctClientMapper.class);
        //job.setCombinerClass(DistinctClientReducer.class);
        job.setReducerClass(DistinctClientReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(distinctOutputPath));
        //System.exit(job.waitForCompletion(true) ? 0 : 1);

        //不需要 job1. 调用 reduce cleanup 方法

        Job job1 = Job.getInstance(conf, "client count job");
        job1.setJarByClass(DistinctClientCntCount.class);
        job1.setMapperClass(ClientCountMapper.class);
        job1.setCombinerClass(ClientCountReducer.class);
        job1.setReducerClass(ClientCountReducer.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(distinctOutputPath));
        FileOutputFormat.setOutputPath(job1, new Path(countOutputPath));

        if(job.waitForCompletion(true)){
            //System.exit(job1.waitForCompletion(true)? 0 : 1 );
            System.exit(0);
        }

    }
}
