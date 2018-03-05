package com.neu.h1BAnalysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

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

public class H1BWageRangeSummarization {

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
		job.setJarByClass(H1BWageRangeSummarizationMapper.class);
		job.setMapperClass(H1BWageRangeSummarizationMapper.class);
		job.setReducerClass(H1BWageRangeSummarizationReducer.class);
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

	public static class H1BWageRangeSummarizationMapper extends Mapper<Object, Text, Text, IntWritable> {

		private Text wageRange = new Text();
		private IntWritable ONE = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// String[] values = value.toString().split(",");
			String[] values = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			if (values == null) {
				return;
			}

			if (!values[1].equals("\"CASE_STATUS\"")) {
				String case_status = values[1].trim();
				if ((case_status.equalsIgnoreCase("\"CERTIFIED\"")
						|| case_status.equalsIgnoreCase("\"CERTIFIED-WITHDRAWN\""))) {
					double employeeWage = Double.parseDouble(values[6]);
					// jobtitle_outValue.set(job_title);
					if (employeeWage >= 0 && employeeWage <= 30000) {
						wageRange.set("0-30000 Success Range");
					} else if (employeeWage > 30000 && employeeWage <= 60000) {
						wageRange.set("30000-60000 Success Range");
					} else if (employeeWage > 60000 && employeeWage <= 70000) {
						wageRange.set("60000-70000 Success Range");
					} else if (employeeWage > 70000 && employeeWage <= 80000) {
						wageRange.set("70000-80000 Success Range");
					} else if (employeeWage > 80000 && employeeWage <= 90000) {
						wageRange.set("80000-90000 Success Range");
					} else if (employeeWage > 90000 && employeeWage <= 100000) {
						wageRange.set("90000-100000 Success Range");
					} else if (employeeWage > 100000) {
						wageRange.set("100000- Above Success Range");
					}

				} else {
					if (!values[6].equals("NA")) {
						double employeeWage = Double.parseDouble(values[6]);
						// jobtitle_outValue.set(job_title);
						if (employeeWage >= 0 && employeeWage <= 30000) {
							wageRange.set("0-30000 Failure Range");
						} else if (employeeWage > 30000 && employeeWage <= 60000) {
							wageRange.set("30000-60000 Failure Range");
						} else if (employeeWage > 60000 && employeeWage <= 70000) {
							wageRange.set("60000-70000 Failure Range");
						} else if (employeeWage > 70000 && employeeWage <= 80000) {
							wageRange.set("70000-80000 Failure Range");
						} else if (employeeWage > 80000 && employeeWage <= 90000) {
							wageRange.set("80000-90000 Failure Range");
						} else if (employeeWage > 90000 && employeeWage <= 100000) {
							wageRange.set("90000-100000 Failure Range");
						} else if (employeeWage > 100000) {
							wageRange.set("100000- Above Failure Range");
						}
					}
				}
				context.write(wageRange, ONE);
			}
		}
	}

	public static class H1BWageRangeSummarizationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable outvalue = new IntWritable();
		private Text outkey = new Text();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable s : values) {
				sum += s.get();
			}
			outvalue.set(sum);
			context.write(key, outvalue);
		}
	}
}
