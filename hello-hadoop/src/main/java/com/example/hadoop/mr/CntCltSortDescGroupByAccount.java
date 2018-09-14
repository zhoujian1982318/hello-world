package com.example.hadoop.mr;

import com.example.hadoop.util.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class CntCltSortDescGroupByAccount extends Configured implements Tool {



    public static class CntCltSortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private LongWritable mapKey = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String txt = value.toString();
            String[] spliturl = txt.split(",");
            Long acctId = Long.valueOf(spliturl[0]);
            Long sum = Long.valueOf(spliturl[1]);
            mapKey.set(sum);
            context.write(mapKey, value);
        }
    }

    public static class CntCltSortReducer extends Reducer<LongWritable, Text, NullWritable, Text>{
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val:values) {
                context.write(NullWritable.get(), val);
            }
        }
    }



    public static class CntCltMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private LongWritable mapKey = new LongWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String txt = value.toString();
            String[] spliturl = txt.split(",");
            //String clientMac = spliturl[0];
            Long acctId  = Long.valueOf(spliturl[4]);
            Integer status = Integer.valueOf(spliturl[5]);
            if(status==3){
                mapKey.set(acctId);
                context.write(mapKey, one);
            }
        }
    }

    public static class CntCltCombiner extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable>{
        private IntWritable sumOutput = new IntWritable();
        @Override
        protected void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0 ;
            for (IntWritable val : values) {
                sum += val.get();
            }
            sumOutput.set(sum);
            context.write(key, sumOutput);
        }
    }

    public static class CntCltReducer extends Reducer<LongWritable, IntWritable, NullWritable, Text>{
        private Text outText = new Text();
        @Override
        protected void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0 ;
            for (IntWritable val : values) {
                sum += val.get();
            }
            String text = String.format("%d,%d", key.get(), sum);
            outText.set(text);
            context.write(NullWritable.get(), outText);
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapreduce.job.jar", "C:\\Users\\Administrator\\git\\hello-world\\hello-hadoop\\target\\hello-hadoop-1.0.jar");
        CntCltSortDescGroupByAccount mrJob = new CntCltSortDescGroupByAccount();
        mrJob.setConf(conf);
        ToolRunner.run(mrJob, args);
    }

    @Override
        public int run(String[] args) throws Exception {
        String inputPath = "/mgmt/cs/client-station.txt";
        String cntCltGroupPath = "/mgmt/cs/output/cnt-clt-group/";
        String cntCltSortPath = "/mgmt/cs/output/cnt-clt-sort/";
        String partitionFilePath = "/mgmt/cs/partition/part_lst";

        FileUtils.deleteFile(getConf(), cntCltGroupPath, true);
        FileUtils.deleteFile(getConf(), partitionFilePath, true);
        FileUtils.deleteFile(getConf(), cntCltSortPath, true);

        //这个一定要在 创建job 的前面， 要不然 job 会读不到这个配置会报 Can't read partitions file
        TotalOrderPartitioner.setPartitionFile(getConf(), new Path(partitionFilePath));

        InputSampler.RandomSampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.1, 1000, 5);

        Job job = Job.getInstance(getConf(), "order group by job");

        job.setJarByClass(CntCltSortDescGroupByAccount.class);
        job.setMapperClass(CntCltMapper.class);
        job.setReducerClass(CntCltReducer.class);
        job.setCombinerClass(CntCltCombiner.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //defautl inputformat class outputformat class
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //default partitioner class
        //job.setPartitionerClass(HashPartitioner.class);

        //global sort by key (id-nmsaccount)
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setNumReduceTasks(3);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(cntCltGroupPath));

        InputSampler.writePartitionFile(job, sampler);

        boolean status = job.waitForCompletion(true);

        if (status) {
            Job job1 = Job.getInstance(getConf(), "sort count job");
            job1.setJarByClass(CntCltSortDescGroupByAccount.class);
            job1.setMapperClass(CntCltSortMapper.class);
            job1.setReducerClass(CntCltSortReducer.class);
            job1.setMapOutputKeyClass(LongWritable.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(NullWritable.class);
            job1.setOutputValueClass(Text.class);
            job1.setSortComparatorClass(LongWritable.DecreasingComparator.class);
            job1.setPartitionerClass(HashPartitioner.class);
            job1.setNumReduceTasks(1);
            FileInputFormat.setInputPaths(job1, new Path(cntCltGroupPath));
            FileOutputFormat.setOutputPath(job1, new Path(cntCltSortPath));
            status = job1.waitForCompletion(true);
            if (status) {
                return 0;
            } else {
                return 1;
            }
        }else{
            return 1;
        }

    }
}
