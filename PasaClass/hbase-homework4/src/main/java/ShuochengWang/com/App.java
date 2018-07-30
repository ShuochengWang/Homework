package ShuochengWang.com;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

class ValueCount extends Configured implements Tool
{
    public static class MyMapper extends TableMapper<IntWritable, IntWritable>
    {
        private final IntWritable ONE = new IntWritable(1);
        private IntWritable keyout = new IntWritable();

        public void map(ImmutableBytesWritable row, Result result, Mapper.Context context) throws IOException, InterruptedException
        {
            byte[] value = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("cq1"));
            keyout.set(Bytes.toInt(value));
            context.write(keyout, ONE);
        }
    }

    public static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
    {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(IntWritable keyin, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int i = 0;
            for (IntWritable val : values)
            {
                i += val.get();
            }
            result.set(i);
            context.write(keyin, result);
        }
    }

    public static Job configureJob(Configuration conf, String[] args) throws IOException
    {
        Job job = new Job(conf,"ValueCount");
        job.setJarByClass(ValueCount.class);     // class that contains mapper and reducer

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        // set other scan attrs

        TableMapReduceUtil.initTableMapperJob(
                "Test",             // input table
                scan,               // Scan instance to control CF and attribute selection
                MyMapper.class,     // mapper class
                IntWritable.class,  // mapper output key
                IntWritable.class,  // mapper output value
                job);
        job.setReducerClass(MyReducer.class);    // reducer class
        job.setNumReduceTasks(2);    // at least one, adjust as required
        FileOutputFormat.setOutputPath(job, new Path("/result"));  // adjust directories as required
        return job;
    }

    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create(getConf());
        Job job = configureJob(conf, args);
        return (job.waitForCompletion(true) ? 0 : 1);
    }
}

public class App {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        int result;
        result = ToolRunner.run(conf, new ValueCount(), args);

        if(result == 0)
        {
            System.out.println("Success");
        }
        else
        {
            System.out.println("Fail");
        }

    }
}
