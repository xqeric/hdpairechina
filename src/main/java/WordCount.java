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

public class WordCount {

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

            String line = value.toString();
            //splite one satance to words..
            StringTokenizer tokenizer = new StringTokenizer(line);

            while(tokenizer.hasMoreTokens()){
                word.set(tokenizer.nextToken().replace("“",""));
                output.collect(word,one);
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
            while (values.hasNext()){

                sum+= values.next().get();
            }
            output.collect(key,new IntWritable(sum));



        }
    }

    public static void main(String[] args) throws Exception{

        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        Path inputpath = new Path("hdfs://Qiang-Think:54310/wordinput/");
        Path outputpath = new Path("hdfs://Qiang-Think:54310/wordoutput/"+new Random().nextInt(100)+"/");
        // new Path(args[0]) new Path(args[1])
        FileInputFormat.setInputPaths(conf,inputpath);
        FileOutputFormat.setOutputPath(conf,outputpath);

        JobClient.runJob(conf);
    }

}
