package bigdata1;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SimpleReduceSideJoin {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text tmpKey = new Text();
		private Text tmpValue = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String val = value.toString();
			String[] cols = val.split("\\^");
			if(cols.length < 4) {
				tmpKey.set(cols[0]);
				tmpValue.set("    " + cols[1] + "    " + cols[2]);
			} else {
				tmpKey.set(cols[2]);
				tmpValue.set(cols[3]);
			}
			context.write(tmpKey, tmpValue);
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
		private Text tmpKey = new Text();
		private DoubleWritable tmpValue = new DoubleWritable();

		@Override
 		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0; // initialize the sum for each keyword
			int count = 0;
			String add = null;
			for (Text val : values) {
				if(isPositiveDouble(val.toString())) {
					sum += Double.parseDouble(val.toString());
					count ++;
				} else 
					add = val.toString();
			}
			tmpKey.set(key.toString() + "    " + add );
			tmpValue.set(sum/count);
			context.write(tmpKey, tmpValue);
		}
		

		private static boolean isPositiveDouble(String arg) {
			int index = arg.indexOf('.');
			if(index == arg.length() - 1) return false;
			
			for(int i=0;i<arg.length();++i)
				if(i == index) continue;
				else if(!Character.isDigit(arg.charAt(i))) return false;
				
			return true;
		}
		
	}
	
	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length < 3) {
			System.err.println("Usage: HW1_Q3 business.csv review.csv <output_dir>");
			System.exit(2);
		}
		
		Job firstJob = new Job(conf, "HW1 - Q3 - job1");

		String tmpoutput = "/sxs140030/tmpout1" + ( new Random().nextInt(10000));
		
		firstJob.setJarByClass(SimpleReduceSideJoin.class);
		//job.setNumReduceTasks(1);

		firstJob.setMapperClass(Map.class);
		firstJob.setReducerClass(Reduce.class);
		
		firstJob.setMapOutputKeyClass(Text.class);
		firstJob.setMapOutputValueClass(Text.class);
		
		firstJob.setOutputKeyClass(Text.class); 
		firstJob.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(firstJob, new Path(otherArgs[0]));
		FileInputFormat.addInputPath(firstJob, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(firstJob, new Path(tmpoutput));

		firstJob.waitForCompletion(true);
		if( firstJob.waitForCompletion(true) ) {
			Job secondJob = new Job(conf, "HW1 - Q3 - job2");

			secondJob.setJarByClass(TopTenMapReduce.class);
			
			secondJob.setMaxMapAttempts(2);
			secondJob.setNumReduceTasks(1);
			
			secondJob.setMapOutputKeyClass(Text.class);
			secondJob.setMapOutputValueClass(Text.class);
			secondJob.setReducerClass(TopTenMapReduce.Reduce.class);
			
			secondJob.setOutputKeyClass(Text.class);
			secondJob.setOutputValueClass(DoubleWritable.class);

			secondJob.setInputFormatClass(KeyValueTextInputFormat.class);
			KeyValueTextInputFormat.addInputPath(secondJob, new Path(tmpoutput + "/part-r-00000"));
			FileOutputFormat.setOutputPath(secondJob, new Path(otherArgs[2]));

			System.exit(secondJob.waitForCompletion(true) ? 0 : 1);
		} else 
			System.exit(1);		
		
		//FileSystem fs = FileSystem.get(conf);
		//fs.delete(new Path(tmpoutput), true)
	}
}
