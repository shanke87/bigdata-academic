package bigdata1;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class TopTenMapReduce {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] cols = value.toString().split("\\^");			
			context.write(new Text(cols[2]), new Text(cols[3]));
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
		static class pair extends AbstractMap.SimpleEntry<Double,String> implements Comparable <pair> {
			private static final long serialVersionUID = 1L;
			public pair(Double key, String value) { 	super(key, value);	}
			public int compareTo(final pair a) { return this.getKey().compareTo(a.getKey()); }
		}
		PriorityQueue<pair> qu = new PriorityQueue<>(10);
	
		@Override
 		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0; // initialize the sum for each keyword
			int count = 0;
			for (Text val : values) {
				sum += Double.parseDouble(val.toString());
				count ++;
			}
			
			if(qu.size() < 10  ) {
				qu.add(new pair(sum/count, key.toString()));
			} else if(qu.size() == 10 && qu.peek().getKey() < (sum/count)) {
				qu.poll();
				qu.add(new pair(sum/count, key.toString()));
			}
			
		}
		
		@Override
 		public void cleanup(Context context) throws IOException, InterruptedException {
			pair[] top10 = new pair[qu.size()];
			int c = 0;
			while(!qu.isEmpty()) top10[c++] = qu.poll();
			while((--c) >= 0) context.write(new Text(top10[c].getValue()), new DoubleWritable(top10[c].getKey() ));
		}
	}
	
	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: HW1_Q2 review.csv <output_dir>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "HW1 - Q2");

		job.setJarByClass(TopTenMapReduce.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
}
