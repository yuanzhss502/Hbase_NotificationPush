package com.yuanzhss.hbase_syllabus.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;


import java.io.IOException;

public class Write2HbaseReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> puts, Context context) throws IOException, InterruptedException {


        for (Put put: puts) {

            context.write(NullWritable.get(), put);

        }

    }
}
