import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class ReducerJoin {
		// Input: sol.txt
		// Swap key and value, sort by key (age), (userId, age)
		public static class AgeMap extends Mapper<LongWritable, Text, LongWritable, Text> { 
			
			private HashMap<String, Integer> age = new HashMap<>();
			public void map(LongWritable key, Text value, Context context) 
					throws IOException,InterruptedException{
					String[] data = value.toString().split("\t");
					if(data.length == 2) {
						String[] friends = data[1].split(",");
			       		int minAge = Integer.MAX_VALUE;
			       		for(String friend: friends) {
			       			if(age.containsKey(friend)) {
			       				if(age.get(friend) < minAge) {
			       					minAge = age.get(friend);
			       				}
			       			}
			       		}
			       		
			       		if(minAge != Integer.MAX_VALUE) {
			       			context.write(new LongWritable(minAge), new Text(data[0]));
			       		}
					}
			}	
			
			@Override
			public void setup(Context context) throws IOException, InterruptedException {
				super.setup(context);
				// Input: userdata.txt
				//Path path = new Path("/inputFile/userdata.txt");
				//read data to memory on the mapper.
				Configuration conf = context.getConfiguration();
				
				Path path=new Path(context.getConfiguration().get("userData"));//Location of file in HDFS
				
				FileSystem fs = FileSystem.get(conf);
				FileStatus[] fss = fs.listStatus(path);
			    for (FileStatus status : fss) {
			        Path pt = status.getPath();
			        
			        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			        String line = br.readLine();
			        
			        while (line != null){
			        	String[] arr = line.split(",");
			        	if (arr.length == 10) {
			        		//put (userId, age) in the HashMap
			        		age.put(arr[0].trim(), calAge(arr[9]));
			        	}
			        	line = br.readLine();
			        }
			    }
			}
			
			// function of calculating age
			public int calAge(String dob) {
				DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
				Date d;
				try {
					d = df.parse(dob);
					Date current = new Date();
					int age = (int) ((current.getTime() - d.getTime())/(1000 * 24 * 60 * 60));
					return (age/365);

				} catch (Exception e) {
				}
				return 0;
			}
		}
			
			// Get top 10 minimum ages:(userId, minage_of_user's_friend +"A")
			public static class SortReducer extends Reducer<LongWritable, Text, Text, Text> {
				private int idx = 0;

				public void reduce(LongWritable key, Iterable<Text> values, Context context)
				      throws IOException, InterruptedException {
					for (Text value : values) {
				    	if (idx < 10) {
				    		idx++;
				    		StringBuilder age = new StringBuilder();
		    	    		age.append("AGE:" + key.toString());
				    		context.write(value, new Text(age.toString()));
				    	}
				    }
				}
			}
			
			// Input: userdata.txt
			// Get user's address: (userId, address,city,state + "B")
			public static class AddressMap extends Mapper<LongWritable, Text, Text, Text> { 
				public void map(LongWritable key, Text value, Context context) 
						throws IOException, InterruptedException {
					String[] userInfo = value.toString().split(",");
					StringBuilder address = new StringBuilder(); 
					if(userInfo.length == 10) {
						address.append("ADDR:"+userInfo[3]+","+userInfo[4]+","+userInfo[5]);
						context.write(new Text(userInfo[0]), new Text(address.toString()));
					}
				}
			}
			
			// Input: data in temp folder from SortReducer output
			public static class TempMap extends Mapper<Text, Text, Text, Text>{
				public void map(Text key, Text value, Context context) 
					throws IOException,InterruptedException{
					context.write(key, value); 		
					}
			}
	
		// Reduce-join files in AgeMap(TempMap) and AddressMap, <id tab details>
		public static class JoinReduce extends Reducer<Text,Text,Text,Text> {
			public void reduce(Text key, Iterable<Text> values, Context context) 
					throws IOException, InterruptedException {
				
				String[] myStringArray = new String[2];
	       		for(Text value: values) { 
	       			
	       			String[] data = value.toString().split(":");
	       			
	       			if(data[0].equals("ADDR")) 
	       				myStringArray[0] = data[1];
	       			
	       			if(data[0].equals("AGE")) 
	       				myStringArray[1] = data[1];
	       			
	       		}
	       		StringBuilder newValue = new StringBuilder();
	       		if( myStringArray[1] != null && !myStringArray[1].isEmpty()) {
	       			newValue.append(myStringArray[0] + "," + myStringArray[1]);
				    context.write(new Text((key.toString()) + (newValue.toString())), new Text());
	       		}	
			}
		}
		
	// Driver Program
    public static void main(String []args) throws Exception {
    	Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
     		if (otherArgs.length != 4) {
     			System.err.println("Usage: ReducerJoin <sol.txt> <userdata.txt> <temp> <out>");
     			System.exit(2);
     		}
     		//String inputPath0 = otherArgs[0];
     		//String inputPath1 = otherArgs[1];
            //String tempPath   = otherArgs[2];
            //String outputPath = otherArgs[3];
     		conf.set("userData", otherArgs[1]);
     		
     		
        // 1st Job
        {
            @SuppressWarnings("deprecation")
			Job job = new Job(conf, "job");
            
            job.setJarByClass(ReducerJoin.class);
            job.setMapperClass(ReducerJoin.AgeMap.class);
            job.setReducerClass(ReducerJoin.SortReducer.class);
            job.setMapOutputKeyClass(LongWritable.class);   //set job1's mapper output key type         
            job.setMapOutputValueClass(Text.class);         //set job1's mapper output value type 
            job.setOutputKeyClass(Text.class);      		// set job1;s output key type
            job.setOutputValueClass(Text.class);            // set job1's output value type
            
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  //set job1's input HDFS path
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));  //job1's output path
            
            // Hadoop by default sorts the output of map by key in ascending order, set it to decreasing order
            job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
 
            if(!job.waitForCompletion(true))
                System.exit(1);
        }
        
        // 2nd Job
        {
            @SuppressWarnings("deprecation")
			Job job2 = new Job(conf, "job2");

            job2.setJarByClass(ReducerJoin.class);
            job2.setReducerClass(ReducerJoin.JoinReduce.class);
            
            MultipleInputs.addInputPath(job2, new Path(otherArgs[1]), TextInputFormat.class, AddressMap.class); 
            MultipleInputs.addInputPath(job2, new Path(otherArgs[2]), KeyValueTextInputFormat.class, TempMap.class); 

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            //job2.setInputFormatClass(KeyValueTextInputFormat.class);
            
            job2.setNumReduceTasks(1);
            
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
