package com.neu.h1BAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class H1BCEOPetitionsPerYear {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		// Categorizing petitions filed for a "CEO" for each year(BINNING ORGANIZATION
		// PATTERN)

		Configuration conf = new Configuration();

		if (args.length != 2) {
			System.err.println("Usage: H1BDataAnalysis <input> <out>");
			System.exit(2);
		}

		Path input = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		Job job = new Job(conf, "H1B Data Analysis");
		job.setJarByClass(H1BCEOPetitionsPerYear.class);
		job.setMapperClass(H1BCEOPetitionsPerYearMapper.class);
		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.setCountersEnabled(job, true);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, outputDir);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		int code = job.waitForCompletion(true) ? 0 : 1;

		System.exit(code);

	}

	public static class H1BCEOPetitionsPerYearMapper extends Mapper<Object, Text, Text, NullWritable> {

		private MultipleOutputs<Text, NullWritable> mos = null;

		protected void setup(Context context) {
			mos = new MultipleOutputs<Text, NullWritable>(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] values = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			if (values == null) {
				return;
			}

			if (!values[7].equalsIgnoreCase("YEAR")) { 
				if (values[4].substring(1, values[4].length()-1).equalsIgnoreCase("CEO") || values[4].substring(1, values[4].length()-1).equalsIgnoreCase("CHIEF EXECUTIVE OFFICER") ||values[4].substring(1, values[4].length()-1).equalsIgnoreCase("CHEIF EXECUTIVE OFFICER")) {
					if (values[7].equalsIgnoreCase("2011")) {
						mos.write("bins", value, NullWritable.get(), "Year-2011");
					} else if (values[7].equalsIgnoreCase("2012")) {
						mos.write("bins", value, NullWritable.get(), "Year-2012");
					} else if (values[7].equalsIgnoreCase("2013")) {
						mos.write("bins", value, NullWritable.get(), "Year-2013");
					} else if (values[7].equalsIgnoreCase("2014")) {
						mos.write("bins", value, NullWritable.get(), "Year-2014");
					} else if (values[7].equalsIgnoreCase("2015")) {
						mos.write("bins", value, NullWritable.get(), "Year-2015");
					} else if (values[7].equalsIgnoreCase("2016")) {
						mos.write("bins", value, NullWritable.get(), "Year-2016");
					}
				}
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
}
