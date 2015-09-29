package bigdata1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SimpleMapSideJoin {
	private static final String BUSINESS_FILE_PATH = "businessFilePath";

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text(); // type of output key
		private Text wordValue = new Text();
		TreeSet<String> businessIds = new TreeSet<String>();
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			FileSystem fileSys = FileSystem.get(conf);
			
	        Path pt=new Path("hdfs://cshadoop1" + conf.get(BUSINESS_FILE_PATH));
	        BufferedReader br=new BufferedReader(new InputStreamReader(fileSys.open(pt)));
	        String line;
	        line=br.readLine();
	        while (line != null){
	                //System.out.println(line);
	                if(line.contains("Stanford")) {
	                	businessIds.add(line.substring(0, line.indexOf('^')));
	                }
	                line = br.readLine();
	        }
	        br.close();
		}
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String strValue = value.toString();
			String[] cols = strValue.split("\\^");
			String businessId = cols[2];
			
			if(businessIds.contains(businessId)) {
				String id = strValue.substring(0, strValue.indexOf('^'));
				word.set(cols[1]);
				wordValue.set(cols[3]);
				context.write(word, wordValue);
			}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 3) {
			System.err.println("Usage: HW1_Q4 <business.csv path> <review.csv path> <output_dir>");
			System.exit(2);
		}
		
		conf.set(BUSINESS_FILE_PATH, otherArgs[0]);

		Job job = new Job(conf, "HW1 - Q4");

		job.setJarByClass(SimpleMapSideJoin.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
}
