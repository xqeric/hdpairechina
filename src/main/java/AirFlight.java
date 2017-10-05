/**
 * Created by qiang on 17-10-3.
 */

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;



public class AirFlight {

    /**
     * the implemetation of map
     */
    public static class Map extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        private Text word = new Text();


        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException{

            //add one verification for csv file
            if(!value.toString().startsWith("Year")) {
                //parse the input value, and convert it to flight data obj.
                FlightData flightData = FlightDataParse.parse(value.toString());
                // add flight number and arrDelay to the queue.

                output.collect(new Text(flightData.getFlightNumber().toString())
                        ,new IntWritable(flightData.getArrDelay()));
            }

        }
    }


    /**
     * the implemetation of reduce
     */
    public static class Reduce extends MapReduceBase
            implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterator<IntWritable> values,OutputCollector<Text,
                IntWritable> output, Reporter reporter) throws IOException{

            int sum = 0;
            int totalNumber = 0;
            while (values.hasNext()){

                sum+= values.next().get();
                totalNumber+=1;
            }
            if(sum>0) {
                int avg = Math.round(sum / totalNumber);

                output.collect(key, new IntWritable(avg));
            }
        }
    }

        public static void main(String[] args) throws Exception {


            JobConf conf = new JobConf(WordCount.class);
            conf.setJobName("my-flight-data");

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(IntWritable.class);

            conf.setMapperClass(Map.class);
            conf.setCombinerClass(Reduce.class);
            conf.setReducerClass(Reduce.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);


            Path inputpath = new Path("hdfs://Qiang-Think:54310/input/");
            Path outputpath = new Path("hdfs://Qiang-Think:54310/output/" + new Random().nextInt(1000) + "/");
            // new Path(args[0]) new Path(args[1])
            // new Path(args[0]) new Path(args[1])
            FileInputFormat.setInputPaths(conf,inputpath);
            FileOutputFormat.setOutputPath(conf,outputpath);

            JobClient.runJob(conf);
        }




}
