import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class MovieGenre {
    
    public static class Map extends Mapper<LongWritable, Text, Text, Text>{
        private final static IntWritable one = new IntWritable(1); 
        private Text word = new Text(); // type of output key
        public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
        
        Configuration conf = context.getConfiguration();
        String p1 = conf.get("genre");
        String s = value.toString();
        String itr[] = s.split("::");
       
            String sub_itr[]= itr[2].split("\\|");
            for(int i=0;i<sub_itr.length;i++)
            {
                if(sub_itr[i].equals(p1))
                {
                    word.set(itr[1]); // set word as each input keyword 
                    context.write(word, new Text(itr[2])); // create a pair <keyword, 1> 
                }
            }
            
       
        
        }
        }
    
    public static class Reduce extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Text values, Context context ) throws IOException, InterruptedException {
        //int sum = 0; // initialize the sum for each keyword 
        //for (IntWritable val : values) { 
        //sum += val.get(); } 
        //result.set(sum);
        context.write(key, values); // create a pair <keyword, number of occurences> 
        }
        }
    
    // Driver program
     public static void main(String[] args) throws Exception { 
     Configuration conf = new Configuration(); 
    conf.set("genre", args[2]);
     //args[2]= "genre";
     String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args 
     if (otherArgs.length != 3) { 
     System.err.println("Usage: WordCount <in> <out>"); 
     System.exit(2); 
     }
    // create a job with name "wordcount" 
    Job job = new Job(conf, "moviegenre"); 
    job.setJarByClass(MovieGenre.class);
    job.setMapperClass(Map.class); 
    job.setReducerClass(Reduce.class);

    // uncomment the following line to add the Combiner 
    job.setCombinerClass(Reduce.class);// set output key type 
    job.setOutputKeyClass(Text.class); // set output value type 
    job.setOutputValueClass(Text.class); //set the HDFS path of the input data 
    FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // set the HDFS path for the output
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    //Wait till job completion 
    System.exit(job.waitForCompletion(true) ? 0 : 1); 

     }




}