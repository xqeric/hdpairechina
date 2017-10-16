import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Calendar;
import java.util.Random;

/**
 * I am going to demo the Reduce and Hbase relation via this example..
 *
 * Author: Qiang
 * Version : 2017 10.14
 *
 * This code was passed testing on the 2017-10-16
 * 
 */

public class FlightMapExample {

    /**
     * mapper
     */
    public static class MyMapper extends Mapper<LongWritable, Text,Text,LongWritable>
    {
        public MyMapper() {
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //add one verification for csv file
            if (!value.toString().startsWith("Year")) {
                //parse the input value, and convert it to flight data obj.

                    String[] fields = value.toString().split(",");
                    String flightNumber = fields[9];

                    Integer depDelay = fields[15].startsWith("NA") ? 0 : Integer.parseInt(fields[15]);

                    context.write(new Text(flightNumber), new LongWritable(depDelay));

            }
        }

    }

    /**
     * execute  create 'air' ,'info'
     */
    public static class MyReducer extends TableReducer<Text, LongWritable, ImmutableBytesWritable>
    {

        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException

        {
            // Add up all of the page views for this hour
            String newString = new String() ;
            long sum = 0;
            for(  LongWritable value: values )
            {
                sum += value.get();
            }


            LongWritable sw = new LongWritable(sum);

            Put put = new Put(key.getBytes());

            // Add the page to the info column family
            put.add( Bytes.toBytes( "info" ),
                    Bytes.toBytes( "delayTime" ),
                    sw.toString().getBytes());
            // Write out the sum
            context.write( null, put);
            //Write current data to Hbase
            //writeHbase(key.toString(),sum);

        }
    }


    public static void main( String[] args )
    {
        try
        {
            // Setup Hadoop
            Configuration conf = HBaseConfiguration.create();//new Configuration();
            Job job = Job.getInstance(conf, "air");
            job.setJarByClass( FlightMapExample.class );

            Path inputpath = new Path("hdfs://Qiang-Think:54310/input/");
            FileInputFormat.setInputPaths(job, inputpath);
            job.setMapperClass(MyMapper.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            job.setOutputFormatClass(NullOutputFormat.class);


            TableMapReduceUtil.initTableReducerJob(
                    "air",
                    MyReducer.class,
                    job);

            // Execute the job
            System.exit( job.waitForCompletion( true ) ? 0 : 1 );

        }
        catch( Exception e )
        {
            e.printStackTrace();
        }
    }



}
