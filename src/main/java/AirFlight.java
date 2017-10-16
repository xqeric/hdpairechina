/**
 * Created by qiang on 17-10-3.
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import static org.apache.hadoop.hdfs.TestBlockStoragePolicy.conf;

public class AirFlight {

    /**
     * the implemetation of map
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);

        private Text word = new Text();


        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {

            //add one verification for csv file
            if (!value.toString().startsWith("Year")) {
                //parse the input value, and convert it to flight data obj.
                FlightData flightData = FlightDataParse.parse(value.toString());
                // add flight number and arrDelay to the queue.

                context.write(new Text(flightData.getFlightNumber().toString())
                        , new Text(String.valueOf(flightData.getArrDelay())));
            }

        }
    }


    /**
     * the implemetation of reduce
     */

    public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable> {


        public void reduce(Text key, Iterator<Text> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            int totalNumber = 0;
            while (values.hasNext()) {

                sum += Integer.parseInt(values.next().toString());
                totalNumber += 1;
            }
            if (sum > 0) {
                int avg = Math.round(sum / totalNumber);

                Put put = new Put(Bytes.toBytes("tg"));
                put.addColumn(Bytes.toBytes("avg"),null,Bytes.toBytes(avg));


//                put.add(Bytes.toBytes("flights")
//                        ,Bytes.toBytes("flightnum")
//                        ,Bytes.toBytes(key.toString()));

                context.write(null, put);
            }
        }
    }

    public static void main(String[] args) throws Exception {


        Configuration config = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum.", "localhost");
        Job job = new Job(conf, "JOB_NAME");

        job.setJarByClass(AirFlight.class);



        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //set up intput path
        Path inputpath = new Path("hdfs://Qiang-Think:54310/input/");
        FileInputFormat.setInputPaths(job, inputpath);
        //job.setOutputFormatClass(NullOutputFormat.class);

       // job.setReducerClass(Reduce.class);

       TableMapReduceUtil.initTableReducerJob(
                "flight",        // output table
                Reduce.class,    // reducer class
                job);

       // job.setNumReduceTasks(1);

        job.waitForCompletion(true);


//        JobConf conf = new JobConf(WordCount.class);
//        conf.setJobName("my-flight-data");
//
//        conf.setOutputKeyClass(Text.class);
//        conf.setOutputValueClass(IntWritable.class);
//
//        conf.setMapperClass(Map.class);
//        conf.setCombinerClass(Reduce.class);
//        conf.setReducerClass(Reduce.class);
//
//        conf.setInputFormat(TextInputFormat.class);
//
//        conf.setOutputFormat(TextOutputFormat.class);
//
//
//        Path inputpath = new Path("hdfs://hadoop-master:54310/input/");
//        Path outputpath = new Path("hdfs://hadoop-master:54310/output/" + new Random().nextInt(1000) + "/");
//        // new Path(args[0]) new Path(args[1])
//        // new Path(args[0]) new Path(args[1])
//        FileInputFormat.setInputPaths(conf, inputpath);
//        FileOutputFormat.setOutputPath(conf, outputpath);
//
//        JobClient.runJob(conf);
    }


}
