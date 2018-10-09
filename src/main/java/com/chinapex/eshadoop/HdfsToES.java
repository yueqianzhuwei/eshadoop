package com.chinapex.eshadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.io.IOException;

public class HdfsToES {
    private static final Log LOG = LogFactory.getLog(HdfsToES.class);
    public static class MyMapper extends Mapper<Object, Text, NullWritable, Text> {
        private Text line=new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
//            byte[] line = value.toString().trim().getBytes();
//            LOG.info("value::"+value.toString());
//            BytesWritable blog = new BytesWritable(line);
//            context.write(NullWritable.get(), blog);
            if(value.getLength()>0){
                line.set(value);
                LOG.info("value:::"+value.toString());
                context.write(NullWritable.get(),line);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.set("es.nodes", "192.168.160.128");
        conf.set("es.port", "9200");
        conf.set("es.nodes.wan.only","true");
        conf.set("es.resource", "eshadoop/esoop");
        conf.set("es.mapping.id", "id");
        conf.set("es.input.json", "yes");

        Job job = Job.getInstance(conf, "hadoop es write test");
        job.setJarByClass(HdfsToES.class);
        job.setMapperClass(MyMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("/opt/standalone/hadoop-2.8.3/eshadoop/user.json"));
        job.waitForCompletion(true);
    }
}
