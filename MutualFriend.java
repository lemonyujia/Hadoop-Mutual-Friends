import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
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

public class MutualFriend {
	/*
	 * This class emits a key(pair of 2 friends) and value (list of friends)
	 */
	public static class Map
	extends Mapper<LongWritable, Text, Text, Text>{
		private Text userPair = new Text();   // type of output key
		private Text friendList = new Text(); // type of output value

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] user = value.toString().split("\t");
			if(user.length == 2) {
				String[] list = user[1].split(",");
				friendList.set(user[1]);
				for(String friend : list) {
					String mapKey;
					// Sort friend pair from (2,0) to (0,2)
					if(Integer.parseInt(user[0]) < Integer.parseInt(friend)) {
							mapKey = user[0] + "," + friend;
					} else {
							mapKey = friend + "," + user[0];
					}
					userPair.set(mapKey);
					context.write(userPair, friendList); // create a pair <friednsPair, friendList>
				}	
			}
		}
	}
	
	/*
	 * Input: a key(pair of friends) and value(2 lists of friends)
	 * Output: the intersection of the 2 lists (mutual friends list)
	 */
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet<String> set = new HashSet<>();
			StringBuilder res = new StringBuilder();
			Boolean visited = false;
			
			String[] ids = key.toString().split(",");
			if((ids[0].equals("0") && ids[1].equals("1")) || 
					(ids[0].equals("20") && ids[1].equals("28193")) ||
					(ids[0].equals("1") && ids[1].equals("29826")) ||
					(ids[0].equals("6222") && ids[1].equals("19272")) ||
					(ids[0].equals("28041") && ids[1].equals("28056"))) 
			{	
				for(Text val : values) {
					String[] temp = val.toString().split(",");
					if(!visited) {
						for(String t : temp) {
							set.add(t);
						}
						visited = true;
					} else {
						for(String t : temp) {
							if(set.contains(t)) {
								res.append(t);
								res.append(",");
							}
						}
					}	
				}
				result = new Text(res.toString());
				context.write(key, result); // create a pair <friendPair, mutual friendList>
			}
		}
	}


	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all arguments
		if (otherArgs.length != 2) {
			System.err.println("Usage: MutualFriend <in> <out>");
			System.exit(2);
		}

		// create a job with name "MutualFriend"
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "MutualFriend");
		job.setJarByClass(MutualFriend.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
