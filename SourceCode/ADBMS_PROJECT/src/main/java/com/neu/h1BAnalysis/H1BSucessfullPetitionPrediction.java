package com.neu.h1BAnalysis;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class H1BSucessfullPetitionPrediction {

	// Predictive yearly analysis of successful certication of the petitions(Summarization,Standard Deviation)

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();

		if (args.length != 2) {
			System.err.println("Usage: H1BDataAnalysis <input> <out>");
			System.exit(2);
		}

		Path input = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		Job job = new Job(conf, "H1B Data Analysis");

		job.setJarByClass(H1BSucessfullPetitionPrediction.class);
		job.setMapperClass(H1BPetitionSucessMapper.class);
		job.setReducerClass(H1BPetitionSucessReducer.class);
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

		if (code == 0) {
			Job job1 = new Job(conf, "H1B Data Analysis");

			job1.setJarByClass(H1BSucessfullPetitionPrediction.class);
			job1.setMapperClass(H1BPetitionSucessPredictiveMapper.class);
			job1.setReducerClass(H1BPetitionSucessPredictiveReducer.class);
			job1.setNumReduceTasks(1);

			job1.setMapOutputKeyClass(NullWritable.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);

			Path finalOutput = new Path("FinalOutput");
			FileInputFormat.addInputPath(job1, outputDir);
			FileOutputFormat.setOutputPath(job1, finalOutput);
			// Delete output if exists
			if (hdfs.exists(finalOutput))
				hdfs.delete(finalOutput, true);
			code = job1.waitForCompletion(true) ? 0 : 1;
		}
		System.exit(code);
	}

	public static class H1BPetitionSucessMapper extends Mapper<Object, Text, Text, IntWritable> {

		private Text outkey = new Text();
		private IntWritable outvalue = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// String[] values = value.toString().split(",");
			String[] values = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

			if (values == null) {
				return;
			}

			if (!values[1].equals("CASE_STATUS")) {
				String case_status = values[1].trim();
				String year = values[7].trim();
				if ((case_status.equalsIgnoreCase("\"CERTIFIED\"")
						|| case_status.equalsIgnoreCase("\"CERTIFIED-WITHDRAWN\""))) {
					outkey.set(year);
					outvalue.set(1);
					context.write(outkey, outvalue);
				}
			}
		}
	}

	public static class H1BPetitionSucessPredictiveMapper extends Mapper<Object, Text, NullWritable, Text> {

		private Text outkey = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// String[] values = value.toString().split(",");
			String[] values = value.toString().split("\t");

			if (values == null) {
				return;
			} else {
				outkey.set(values[0] + "\t" + values[1]);
				context.write(NullWritable.get(), outkey);
			}
		}
	}

	public static class H1BPetitionSucessReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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

	public static class H1BPetitionSucessPredictiveReducer extends Reducer<NullWritable, Text, Text, IntWritable> {

		private IntWritable outvalue = new IntWritable();
		private TreeMap<String, Integer> successRange = new TreeMap<String, Integer>();
		private Text result = new Text();

		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			successRange.clear();
			// For each input value
			for (Text t : values) {
				
				String[] value = t.toString().split("\t");
				successRange.put(value[0], Integer.parseInt(value[1]));
				count++;
				sum += Integer.parseInt(value[1]);
			}

			float mean = sum / count;
			float sumOfSquares = 0.0f;
			for (Entry<String, Integer> entry : successRange.entrySet()) {
				sumOfSquares += (entry.getValue() - mean) * (entry.getValue() - mean);
			}
			float stdDev = (float) Math.sqrt(sumOfSquares / (count - 1));
			Entry<String, Integer> lastEntry = successRange.lastEntry();
			String nextYear = String.valueOf(Integer.parseInt(lastEntry.getKey()) + 1);
			float predictedSucessPetitionValue = lastEntry.getValue() + stdDev;
			successRange.put(nextYear, (int) predictedSucessPetitionValue);
			for (Entry<String, Integer> entry : successRange.entrySet()) {
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			}
		}
	}
}