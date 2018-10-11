import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InMemory {

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String,String> map = new HashMap<String,String>();
		private Text userPair = new Text();   // type of output key
		private Text friendList = new Text(); // type of output value

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] user = value.toString().split("\t");
			String userId1 = context.getConfiguration().get("UserId1");
			String userId2 = context.getConfiguration().get("UserId2");
			
			if (user.length == 2 && (user[0].equals(userId1) || user[0].equals(userId2))) {
				String[] list = user[1].split(",");
				StringBuilder friendData = new StringBuilder();
			    for(String l : list) {
			    	if (map.containsKey(l)){
			    		// set (userId, firstName:state) as output value
						friendData.append(l + ',' + map.get(l)); 
						friendData.append(";");
			    	}
			    }
			    friendList.set(friendData.toString());
			    	
				for(String friend : list) {
					String mapKey;
					// Sort friend pair from (2,0) to (0,2)
					if(Integer.parseInt(user[0]) < Integer.parseInt(friend))
						mapKey = user[0] + "," + friend;
					else 
						mapKey = friend + "," + user[0];
					userPair.set(mapKey);
					context.write(userPair, friendList); // create a pair <friednsPair, friendDataList>
				}	
			}
		}
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			//read data to memory on the mapper.
			Configuration conf = context.getConfiguration();
			//String filePath = conf.get("/inputQ3_1/userdata.txt");
			Path part = new Path(context.getConfiguration().get("userDataFilePath")); //Location of file in HDFS
			
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) {
		        Path pt = status.getPath();
		        
		        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line = br.readLine();
		        while (line != null){
		        	String[] arr = line.split(",");
		        	if (arr.length == 10) {
		        		//put (userId, lastName:state) in the HashMap variable
		        		String data = arr[1] + ":" + arr[5];
		        		map.put(arr[0].trim(), data);
		        	}
		        	line = br.readLine();
		        }
		    }
		}
	}
	
	
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			String userId1 = context.getConfiguration().get("UserId1");
			String userId2 = context.getConfiguration().get("UserId2");
			
			HashSet<String> set = new HashSet<>();
			StringBuilder res = new StringBuilder();
			Boolean visited = false;
			
			String[] keys = key.toString().split(",");
			if(userId1.equals(keys[0]) && userId2.equals(keys[1])) {
				for(Text val : values) {
					String[] temp = val.toString().split(";");
					if(!visited) {
						for(String t : temp) {
							String[] tt = t.toString().split(",");
							set.add(tt[0]);
						}
						visited = true;
					} else {
						for(String t : temp) {
							String[] tt = t.toString().split(",");
							if(set.contains(tt[0])) {
								res.append(tt[1]);
								res.append(",");
							}
						}
					}	
				}
				String temp = res.toString();
				temp = temp.substring(0,temp.length()-1);
				temp = "[" + temp + "]";
				result = new Text(temp);
				context.write(key, result); // create a pair <friendPair, mutual friendDataList>
			}
		}
	}


	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 5) {
			System.err.println("Usage: InMemory <in> <out> <path> <userid1> <userid2>");
			System.exit(2);
		}
		conf.set("userDataFilePath", otherArgs[2]);
		conf.set("UserId1", otherArgs[3]);
		conf.set("UserId2", otherArgs[4]);

		// create a job with name "InMemory"
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "wordid");
		job.setJarByClass(InMemory.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

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