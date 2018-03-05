package com.neu.h1BAnalysis;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class H1BTop10ApplnsPerEmployeePerYear {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		// Top 10 employers to file the most petitions(TOP N FILTERING PATTERN)
		Configuration conf = new Configuration();

		if (args.length != 2) {
			System.err.println("Usage: H1BDataAnalysis <input> <output>");
			System.exit(2);
		}

		Path input = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		Job job = new Job(conf, "H1BDataAnalysis Top 10 Employers");
		job.setJarByClass(H1BApplnsPerEmployeePerYearMapper.class);

		job.setMapperClass(H1BApplnsPerEmployeePerYearMapper.class);
		job.setPartitionerClass(CustomPartioner.class);
		job.setReducerClass(H1BApplnsPerEmployeePerYearReducer.class);
		job.setNumReduceTasks(7);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, outputDir);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		int code = job.waitForCompletion(true) ? 0 : 1;

		System.exit(code);

	}

	public static class CustomPartioner extends Partitioner<Text, LongWritable> {

		@Override
		public int getPartition(Text key, LongWritable value, int numReduceTasks) {
			String[] str = key.toString().split("\t");
			if (str[1].equals("2011"))
				return 0;
			if (str[1].equals("2012"))
				return 1;
			if (str[1].equals("2013"))
				return 2;
			if (str[1].equals("2014"))
				return 3;
			if (str[1].equals("2015"))
				return 4;
			if (str[1].equals("2016"))
				return 5;
			else
				return 6;
		}
	}

	public static class H1BApplnsPerEmployeePerYearMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		LongWritable one = new LongWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (key.get() > 0)

			{
				String[] values = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				if (!values[1].equals("NA") && values[1] != null && !values[2].equals("NA") && values[2] != null
						&& !values[7].equals("NA") && values[7] != null) {
					Text answer = new Text(values[2].replaceAll("\"", "") + "\t" + values[7]);
					context.write(answer, one);
				}
			}
		}
	}

	public static class H1BApplnsPerEmployeePerYearReducer extends Reducer<Text, LongWritable, NullWritable, Text> {
		private TreeMap<LongWritable, Text> topEmployers = new TreeMap<LongWritable, Text>();
		long sum = 0;

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			topEmployers.put(new LongWritable(sum), new Text(key + "," + sum));
			if (topEmployers.size() > 10)
				topEmployers.remove(topEmployers.firstKey());

		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Text t : topEmployers.descendingMap().values())
				context.write(NullWritable.get(), t);
		}
	}
}
