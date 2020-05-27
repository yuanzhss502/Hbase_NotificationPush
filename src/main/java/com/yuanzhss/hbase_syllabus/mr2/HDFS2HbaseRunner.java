package com.yuanzhss.hbase_syllabus.mr2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFS2HbaseRunner implements Tool {

    private Configuration conf = null;

    @Override
    public int run(String[] args) throws Exception {

        //创建job
        Job job = Job.getInstance(conf);
        job.setJarByClass(HDFS2HbaseRunner.class);

        //组装job
        //设置mapper
        job.setMapperClass(ReadFruitFromHDFSMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置reducer
        TableMapReduceUtil.initTableReducerJob("fruit",
                Write2HbaseReducer.class,
                job);

        //inputFormat
        FileInputFormat.addInputPath(job, new Path("/input_fruit"));

        return job.waitForCompletion(true) ?0:1;


    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = HBaseConfiguration.create(conf);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) {
        try {
            int status = ToolRunner.run(new HDFS2HbaseRunner(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}


