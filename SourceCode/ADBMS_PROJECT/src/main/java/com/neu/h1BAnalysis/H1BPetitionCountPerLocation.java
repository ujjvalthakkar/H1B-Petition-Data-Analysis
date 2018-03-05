package com.neu.h1BAnalysis;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class H1BPetitionCountPerLocation {

	// Counting number of petitions based on Location(Counting with counters)

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
		job.setJarByClass(H1BPetitionCountPerLocation.class);
		job.setMapperClass(CountPetitionsPerLocationMapper.class);
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, outputDir);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		int code = job.waitForCompletion(true) ? 0 : 1;

		if (code == 0) {
			System.out.println("OUTPUT VALUES:\nSTATE\tNUMBER OF PETITIONS");
			for (Counter counter : job.getCounters().getGroup(CountPetitionsPerLocationMapper.STATE_COUNTER_GROUP)) {
				System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
			}
		}

		FileSystem.get(conf).delete(outputDir, true);

	}

	public static class CountPetitionsPerLocationMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

		public static final String STATE_COUNTER_GROUP = "State";
		public static final String UNKNOWN_COUNTER = "Unknown";
		
		String[] statesArray = new String[] { "CALIFORNIA", "ALABAMA", "ARKANSAS", "ARIZONA", "ALASKA", "COLORADO",
				"CONNECTICUT", "DELAWARE", "FLORIDA", "GEORGIA", "HAWAII", "IDAHO", "ILLINOIS", "INDIANA", "IOWA",
				"KANSAS", "KENTUCKY", "LOUISIANA", "MAINE", "MARYLAND", "MASSACHUSETTS", "MICHIGAN", "MINNESOTA",
				"MISSISSIPPI", "MISSOURI", "MONTANA", "NEBRASKA", "NEVADA", "NEW HAMPSHIRE", "NEW JERSEY", "NEW MEXICO",
				"NEW YORK", "NORTH CAROLINA", "NORTH DAKOTA", "OHIO", "OKLAHOMA", "OREGON", "PENNSYLVANIA",
				"RHODE ISLAND", "SOUTH CAROLINA", "SOUTH DAKOTA", "TENNESSEE", "TEXAS", "UTAH", "VERMONT", "VIRGINIA",
				"WASHINGTON", "WEST VIRGINIA", "WISCONSIN", "WYOMING" };
		HashSet<String> states = new HashSet<String>(Arrays.asList(statesArray));

		@Override
		public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {

			String[] values = line.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			if (values == null) {
				return;
			}

			String state = values[8].substring(values[8].indexOf(',') + 2, values[8].length() - 1);

			if (state != null && !state.isEmpty() && !state.equalsIgnoreCase("WORKSITE")) {
				boolean unknown = true;
				if (states.contains(state)) {
					context.getCounter(STATE_COUNTER_GROUP, state).increment(1);
					unknown = false;
				}
				if (unknown) {
					context.getCounter(UNKNOWN_COUNTER, state).increment(1);
				}
			}
		}
	}
}
