package bigdata1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SimpleMap {
	public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
		private Text word = new Text(); // type of output key

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String strValue = value.toString();
			if(strValue.contains("Palo Alto")) {
				String id = strValue.substring(0, strValue.indexOf('^'));
				word.set(id);
				context.write(word, NullWritable.get());
			}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: HW1_Q1 <input_file> <output_dir>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "HW1 - Q1");

		job.setJarByClass(SimpleMap.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
}
