
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount {
	private static int Max = 0;
	public static class TokenizerMapper extends Mapper<Object, Text ,Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
		private Text word = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(value.toString(), " ");
			System.out.println(st);
			if(st.hasMoreTokens()) {
				word.set(st.nextToken());
				context.write(word, ONE);
			}
			
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value : values) {
				sum += value.get();
			}
			Max = Math.max(Max, sum);
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {		
		Configuration cnf = new Configuration();
		Job job = Job.getInstance(cnf, "wordcount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

// How to create ... 
// scr -> new Cloass 

// MainFolder --> Build Path --> use External Archive
// file --> user --> lib --> Hadoop
// hadoop.common.jar
// Hadoop.core-2.6.0.mr-----23.jr ( 3rd from Top )

//  Export MainFOlder as--->>>> JAR -->>
//  hadoop fs -put ip.txt input.txt 
//  hadoop jar WordCout.jar WordCount input.txt dir5
// hadoop fs -cat dir5/part-r-00000 