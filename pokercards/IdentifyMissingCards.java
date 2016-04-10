package pokercards;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class IdentifyMissingCards {
	public static class IdentifyMissingCardsMapper extends Mapper<LongWritable,Text,Text,Text>
	{		
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			context.write(new Text(line.split(":")[0]),
					new Text(line.split(":")[1]));	
		}
	}
	public static class IdentifyMissingCardsReducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text>values, Context context) throws IOException,InterruptedException	{	
			
			String[] cards = {"A","1","2","3","4","5","6","7","8","9","10","J","Q","K"}; 
			boolean t;
			ArrayList<String> numbers=new ArrayList<String>();
			for(Text value : values){
				numbers.add(value.toString());
			}
			for (int i=0;i<cards.length;i++){
				t = numbers.contains(cards[i]);
				if(!t){
					context.write(key,new Text(cards[i]));
				}
			}
		}
	}
	public static void main(String[] args) throws Exception  
	{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf,"Missing Cards");
	    job.setJarByClass(IdentifyMissingCards.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.setMapperClass(IdentifyMissingCardsMapper.class);
	    job.setReducerClass(IdentifyMissingCardsReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class); 
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
