package com.neu.h1BAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class H1BFullTimeCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		// Normal Counting of Number of Full time and Part time Petitions
		
		Configuration conf = new Configuration();

		if (args.length != 2) {
			System.err.println("Usage: H1BDataAnalysis <input> <out>");
			System.exit(2);
		}

		Path input = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		Job job = new Job(conf, "H1B Data Analysis");
		job.setJarByClass(H1BSucessFailureMapper.class);
		job.setMapperClass(H1BSucessFailureMapper.class);
		job.setReducerClass(H1BSucessFailureReducer.class);
		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, outputDir);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		int code = job.waitForCompletion(true) ? 0 : 1;

		System.exit(code);

	}

	public static class H1BSucessFailureMapper extends Mapper<Object, Text, Text, IntWritable> {

		private Text outkey = new Text();
		private IntWritable outvalue = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//String[] values = value.toString().split(",");
			String[] values = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			if (values == null) {
				return;
			}

			if (!values[1].equals("\"CASE_STATUS\"") ) {

				if (values[5].equalsIgnoreCase("\"Y\""))
					outkey.set("FULL TIME");
				else if (values[5].equalsIgnoreCase("\"N\""))
					outkey.set("PART TIME");
				outvalue.set(1);
				context.write(outkey, outvalue);
			}
		}
	}

	public static class H1BSucessFailureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable outvalue = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			// For each input value
			for (IntWritable t : values) {
				sum += t.get();
			}
			outvalue.set(sum);
			context.write(key, outvalue);
		}
	}
}
