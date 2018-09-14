package com.example.hadoop.mr;

import com.example.hadoop.util.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SelectMrJob extends Configured implements Tool {


    public static class SelectClauseMapper extends
            Mapper<LongWritable, Text, LongWritable, IntWritable> {
        private LongWritable outKey = new LongWritable();
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String tmp = value.toString();
            String[] arrays = tmp.split(",");
            long acctId = Long.parseLong(arrays[0]);
            outKey.set(acctId);
            context.write(outKey, one);

        }
    }

    public static class SelectClauseReducer extends
                         Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable value = new IntWritable();
            int sum = 0 ;
            for (IntWritable val: values) {
                sum+=val.get();
            }
            value.set(sum);
            context.write(key, value);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String inputPath = "/mgmt/ap/ap.txt";
        String outputPath = "/mgmt/ap/output/select/";
        FileUtils.deleteFile(getConf(),outputPath, true);

        Job job = Job.getInstance(getConf(), "only map job");
        job.setJarByClass(SelectMrJob.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(SelectClauseMapper.class);
        job.setCombinerClass(SelectClauseReducer.class);
        job.setReducerClass(SelectClauseReducer.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        // 如果设置了 reduce task 为 0 的话， 才会没有 reduce 操作。  这样即使设置了combiner class. 和 reduce class 也是没有效果的. 只有map 操作的输出不会对 key 排序
        //似乎combine的操作要 有reduce job 才会执行
        // 如果没有设置 reduce task 为 0 的话。 即使不设置 reduce class. 似乎也会进行reduce 操作(默认)。  输出会对 key 排序

        //map shuffle 阶段： 分区，排序.(spill)溢出. combiner
        //reduce shuffle. 从mapper 节点取不同划分。 (排序。 combiner.) 顺序未知 ？？  最终reduce 方法。收到的键是排序

        //job.setNumReduceTasks(0);

        /** setSortComparatorClass 的作用:
         *   1、map shuffle 阶段。 对每个分区的数据进行排序
         *   2、reduce shuffle 阶段 调用 sortComparator 进行排序， 因为一个reducer接受多个mappers，需要重新排序

             如果没有设置 setSortComparatorClass. 会调用 key值的 注册的默认Comparator . 如：
             LongWritable ：
                static {                                       // register default comparator
                        WritableComparator.define(LongWritable.class, new Comparator());
            }

           setGroupingComparatorClass 的作用
           在reduce 端会对键进行聚拢。这时就要用到分组. 如果compare 为 0， 就属于同一个分组。  同一分组的值会调用同一reduce方法。
           所以如果同一reduce 的键的第一排序字段 都相等。 要对第二字段排序。 需要设置 setGroupingComparatorClass.
           如果没有设置 setGroupingComparatorClass。 默认分组 和 sortComparator 相等的
        **/
        boolean status = job.waitForCompletion(true);

        if (status) {
            return 0;
        } else {
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.jar", "C:\\Users\\Administrator\\git\\hello-world\\hello-hadoop\\target\\hello-hadoop-1.0.jar");
        SelectMrJob mrJob = new SelectMrJob();
        mrJob.setConf(conf);
        ToolRunner.run(mrJob, args);
    }
}
