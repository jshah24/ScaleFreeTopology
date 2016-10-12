import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class CountofUsers {
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		//private final static IntWritable new_var = new IntWritable(1);
		private Text word = new Text(); // type of output key
		public void map(LongWritable key, Text value, Context context
		) throws IOException, InterruptedException {
		
			String s = value.toString();
			String itr[] = s.split("::");
			
			if(itr[1].equals("M"))
			{
				if(Integer.parseInt(itr[2])==7)
				{
					word.set(" M " + itr[2]);
					context.write(word,one);
				}
				
				if(Integer.parseInt(itr[2])==24)
				{
					word.set(" M " + itr[2]);
					context.write(word,one);
				}
				
				if(Integer.parseInt(itr[2])==31)
				{
					word.set(" M " + itr[2]);
					context.write(word,one);
				}
				
				if(Integer.parseInt(itr[2])==41)
				{
					word.set(" M " + itr[2]);
					context.write(word,one);
				}
				
				if(Integer.parseInt(itr[2])==51)
				{
					word.set(" M " + itr[2]);
					context.write(word,one);
				}
				
				if(Integer.parseInt(itr[2])==56)
				{
					word.set(" M " + itr[2]);
					context.write(word,one);
				}
				
				if(Integer.parseInt(itr[2])==62)
				{
					word.set(" M " + itr[2]);
					context.write(word,one);
				}
			}
			if(itr[1].equals("F"))
			{
				if(Integer.parseInt(itr[2])==7)
				{
					word.set(" F " + itr[2]);
					context.write(word,one);
				}
				
				if(Integer.parseInt(itr[2])==24)
				{
					word.set(" F " + itr[2]);
					context.write(word,one);
				}
				
				if(Integer.parseInt(itr[2])==31)
				{
					word.set(" F " + itr[2]);
					context.write(word,one);
				}
				
				if(Integer.parseInt(itr[2])==41)
				{
					word.set(" F " + itr[2]);
					context.write(word,one);
				}
				
				if(Integer.parseInt(itr[2])==51)
				{
					word.set(" F " + itr[2]);
					context.write(word,one);
				}
				
				if(Integer.parseInt(itr[2])==56)
				{
					word.set(" F " + itr[2]);
					context.write(word,one);
				}
				
				if(Integer.parseInt(itr[2])==62)
				{
					word.set(" F " + itr[2]);
					context.write(word,one);
				}
			}

			

			
		}
	}
	
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
		int sum = 0; // initialize the sum for each keyword 
		for (IntWritable val : values) { 
		sum += val.get(); } 
		result.set(sum);
		context.write(key, result); // create a pair <keyword, number of occurences> 
		}
		}

	//Driver program
	public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	//get all args
	if (otherArgs.length != 2) {
	System.err.println("Usage: CountofUser <in> <out>");
	System.exit(2);
	}
	
	Job job = new Job(conf, "countofuser");
	job.setJarByClass(CountofUsers.class);
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	//uncomment the following line to add the Combiner
	job.setCombinerClass(Reduce.class);
	//set output key type
	job.setOutputKeyClass(Text.class);
	//set output value type
	job.setOutputValueClass(IntWritable.class);
	//set the HDFS path of the input data
	FileInputFormat.addInputPath(job, new Path((otherArgs[0])));


	//set the HDFS path for the output
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	//Wait till job completion
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}