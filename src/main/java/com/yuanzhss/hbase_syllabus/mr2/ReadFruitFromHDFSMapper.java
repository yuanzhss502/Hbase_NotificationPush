package com.yuanzhss.hbase_syllabus.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class ReadFruitFromHDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        ArrayList<String> list = new ArrayList<>();

        String[] splits = value.toString().split("\t");
        byte[] rockey = Bytes.toBytes(splits[0]);
        byte[] name = Bytes.toBytes(splits[1]);
        byte[] color = Bytes.toBytes(splits[2]);

        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(rockey);

        Put put = new Put(rockey);

        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), name);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), color);

        context.write(immutableBytesWritable, put);



    }
}
