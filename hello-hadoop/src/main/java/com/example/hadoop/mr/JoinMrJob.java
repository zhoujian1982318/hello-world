package com.example.hadoop.mr;

import com.example.hadoop.util.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class JoinMrJob extends Configured implements Tool {

    public static class AcctMapper extends Mapper<LongWritable, Text, JoinKey, Text>{

        private Text outValue = new Text();
        private Text outAcctName = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String txt = value.toString();
            String []  strArr = txt.split(",");
            long tmp = Long.valueOf(strArr[0]);
            String acctName = strArr[1];
            String tmpTxt = String.format("%d,%s", tmp, acctName);
            outAcctName.set(acctName);
            JoinKey outKey = new JoinKey(tmp,JoinKey.TYPE_ACCT, outAcctName);
            outValue.set(tmpTxt);
            context.write(outKey, outValue);
        }
    }

    public static class ApMapper extends Mapper<LongWritable, Text, JoinKey, Text>{
        private LongWritable accountId = new LongWritable();
        private IntWritable tblType = new IntWritable(2);
        private Text outValue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String txt = value.toString();
            String []  strArr = txt.split(",");
            long tmp = Long.valueOf(strArr[0]);
            String apMac = strArr[1];
            String apName = strArr[2];
            String tmpTxt = String.format("%s,%s", apMac, apName);
            JoinKey outKey = new JoinKey(tmp,JoinKey.TYPE_AP);
            outValue.set(tmpTxt);
            context.write(outKey, outValue);
        }
    }

    public static  class  JoinReducer extends Reducer<JoinKey, Text, NullWritable, Text>{
        private Text outValue = new Text();
        @Override
        protected void reduce(JoinKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String acctName = "unkown";
            for (Text val:values) {
                if(key.getType()==JoinKey.TYPE_ACCT){
                    acctName = key.getAcctName().toString();
                    continue;
                }
                if(key.getType() == JoinKey.TYPE_AP){
                    String txt = val.toString();
                    String [] strArry = txt.split(",");
                    String output = String.format("%s,%s,%d,%s", strArry[0], strArry[1], key.getAcctId(),acctName);
                    outValue.set(output);
                    context.write(NullWritable.get(),outValue);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.jar", "C:\\Users\\Administrator\\git\\hello-world\\hello-hadoop\\target\\hello-hadoop-1.0.jar");
        JoinMrJob mrJob = new JoinMrJob();
        mrJob.setConf(conf);
        ToolRunner.run(mrJob, args);
    }

    @Override
    public int run(String[] args) throws Exception {

        String apInputPath = "/mgmt/ap/ap.txt";
        String acctInputPath = "/mgmt/account/account.txt";
        String joinOutputPath = "/mgmt/cs/output/ap-acct-join/";

        FileUtils.deleteFile(getConf(), joinOutputPath, true);

        Job job = Job.getInstance(getConf(), "join  job");
        job.setJarByClass(JoinMrJob.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job, new Path(acctInputPath),
                TextInputFormat.class, AcctMapper.class);
        MultipleInputs.addInputPath(job, new Path(apInputPath),
                TextInputFormat.class, ApMapper.class);

        job.setMapOutputKeyClass(JoinKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(JoinReducer.class);
        //need set group comparator
        job.setGroupingComparatorClass(JoinKeyGroupComparator.class);
        FileOutputFormat.setOutputPath(job, new Path(joinOutputPath));
        boolean status = job.waitForCompletion(true);
        if(status){
            return 0;
        }else {
            return 1;
        }
    }
}
