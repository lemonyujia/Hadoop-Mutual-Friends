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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Top10 {
	// First job is a standard MutualFriend program
	
	/*
	 * This class emits a key(pair of 2 friends) and value (list of friends)
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private Text userPair = new Text();   // type of output key
		private Text friendList = new Text(); // type of output value

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] user = value.toString().split("\t");
			if(user.length == 2) {
				String[] list = user[1].split(",");
				friendList.set(user[1]);
				for(String friend : list) {
					String mapKey;
					// Sort friend pair from (2,0) to (0,2)
					if(Integer.parseInt(user[0]) < Integer.parseInt(friend))
						mapKey = user[0] + "," + friend;
					else 
						mapKey = friend + "," + user[0];
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
	public static class Reduce extends Reducer<Text,Text,Text,IntWritable> {
		private IntWritable number = new IntWritable();
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			HashSet<String> set = new HashSet<>();
			int counter = 0;
			Boolean visited = false;
			
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
							counter++;
						}
					}
				}	
			}
			number.set(counter);
			context.write(key, number); // create a pair <friendPair, length of mutual friendList>
		}
	}
	
	// Job2's mapper swap key and value, sort by key (the number of mutual friends)
	public static class Mapper2 extends Mapper<Text, Text, LongWritable, Text> {
		private LongWritable number = new LongWritable();
		
		  public void map(Text key, Text value, Context context) 
				  throws IOException, InterruptedException {
		    int newVal = Integer.parseInt(value.toString());
		    number.set(newVal);
		    context.write(number, key);
		  }
		}
	
	// Output the top 10 number of mutual friends 
	public static class Reducer2 extends Reducer<LongWritable, Text, Text, LongWritable> {
		private int idx = 0;

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
		      throws IOException, InterruptedException {
			
			for (Text value : values) {
		    	if (idx < 10) {
		    		idx++;
		    		context.write(value, key);
		    	}
		    }
		  }
		}
	
	
    public static void main(String []args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
     		if (otherArgs.length != 3) {
     			System.err.println("Usage: MutualFriends <in> <out> <temp>");
     			System.exit(2);
     		}
     		String inputPath = otherArgs[0];
            String outputPath = otherArgs[1];
            String tempPath = otherArgs[2];
     		
        //First Job
        {	//create first job
            conf = new Configuration();
            @SuppressWarnings("deprecation")
			Job job = new Job(conf, "Top10");

            job.setJarByClass(Top10.class);
            job.setMapperClass(Top10.Map.class);
            job.setReducerClass(Top10.Reduce.class);
            
            //set job1's mapper output key type
            job.setMapOutputKeyClass(Text.class);
            //set job1's mapper output value type
            job.setMapOutputValueClass(Text.class);
            
            // set job1;s output key type
            job.setOutputKeyClass(Text.class);
            // set job1's output value type
            job.setOutputValueClass(Text.class);
            //set job1's input HDFS path
            FileInputFormat.addInputPath(job, new Path(inputPath));
            //job1's output path
            FileOutputFormat.setOutputPath(job, new Path(tempPath));

            if(!job.waitForCompletion(true))
                System.exit(1);
        }
        //Second Job
        {
            conf = new Configuration();
            @SuppressWarnings("deprecation")
			Job job2 = new Job(conf, "Top10");

            job2.setJarByClass(Top10.class);
            job2.setMapperClass(Top10.Mapper2.class);
            job2.setReducerClass(Top10.Reducer2.class);
            
            //set job2's mapper output key type
            job2.setMapOutputKeyClass(LongWritable.class);
            //set job2's mapper output value type
            job2.setMapOutputValueClass(Text.class);
            
            //set job2's output key type
            job2.setOutputKeyClass(Text.class);
            //set job2's output value type
            job2.setOutputValueClass(LongWritable.class);

            job2.setInputFormatClass(KeyValueTextInputFormat.class);
            
            //hadoop by default sorts the output of map by key in ascending order, set it to decreasing order
            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            job2.setNumReduceTasks(1);
            //job2's input is job1's output
            FileInputFormat.addInputPath(job2, new Path(tempPath));
            //set job2's output path
            FileOutputFormat.setOutputPath(job2, new Path(outputPath));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
