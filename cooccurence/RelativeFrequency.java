package cooccurence;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class RelativeFrequency {
	
	static HashMap<String,Integer> totalOccurence=new HashMap<String,Integer>();
	
	public static class totalOccurenceMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
		public void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException{
			String line=value.toString();
			line = line.replaceAll("[^A-Za-z0-9 ]+","");
			//line=line.replaceAll(". ", " ");
			//line=line.replaceAll("\\? ", " ");
			//line=line.replaceAll("! ", " ");
			//line=line.replaceAll("\\t","");
			//line=line.trim();
			if(line.contains("Function") && line.contains("Tourism"))
				System.out.println(line);
			//System.out.println(key.toString() + ":" +line);
			StringTokenizer token=new StringTokenizer(line," ");
			List<String> words=new ArrayList<String>();
			while(token.hasMoreElements()){
				String temp=new String(token.nextToken());
				temp=temp.replaceAll("\\t", "");
				temp=temp.trim();
				words.add(temp);
			}
			for (int i=1;i<words.size();i++){
				
				context.write(new Text(words.get(i-1).trim()+" "+words.get(i).trim()), new LongWritable(1));
			}
		}
	}
	
	
	public static class totalOccurenceReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
		
		public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException{
			int count=0;
			for (LongWritable val: values){
				count=count+1;
			}
			String temp=new String(key.toString().split(" ")[0].trim());
			if (totalOccurence.containsKey(temp)){
				totalOccurence.put(temp, totalOccurence.get(temp)+count);
			}
			else{
				totalOccurence.put(temp,count);
			}
			context.write(key, new LongWritable(count));
			
		}
	}
	
	public static class RelativeFrequencyMapper extends Mapper<LongWritable,Text,FloatWritable,Text>{
		public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException{
	
			String line=value.toString();
			line=line.trim();
			
			try{
				
				String wordPair1=line.split(" ")[0];
			String wordPair2=line.split(" ")[1].split("\\t")[0];
			float countAB=(float)Integer.parseInt(line.split(" ")[1].split("\\t")[1]);
			float countA=((float)totalOccurence.get(wordPair1))-countAB;
			float relFreq=0;
			if (countA!=0)
			{
				relFreq=countAB/countA;
			}
			context.write(new FloatWritable(relFreq), new Text(wordPair1+" "+wordPair2));
			
			}
			catch(Exception e){
				e.printStackTrace();
				System.out.println(line.toString());
			}
			
			
		}
	}
	
	public static class RelativeFrequencyReducer extends Reducer<FloatWritable,Text,FloatWritable,Text>{
		static int count=100;
		public void reduce(FloatWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			for(Text val:values){
				if (count>0){
					context.write(key, val);
					count=count-1;
				}
				else{
					break;
				}	
			}
		}
	}
	
	public static void main(String []args)throws Exception{
		
		Configuration conf=new Configuration();
		Job pairWords=new Job(conf,"Word Pairs");
		pairWords.setJarByClass(RelativeFrequency.class);
		FileInputFormat.addInputPath(pairWords, new Path(args[0]));//input1
		FileOutputFormat.setOutputPath(pairWords,new Path(args[1]));//output1 & input2
		pairWords.setMapperClass(totalOccurenceMapper.class);
		pairWords.setReducerClass(totalOccurenceReducer.class);
		pairWords.setMapOutputKeyClass(Text.class);
		pairWords.setMapOutputValueClass(LongWritable.class);
		pairWords.setOutputKeyClass(Text.class);
		pairWords.setOutputValueClass(FloatWritable.class);
		pairWords.waitForCompletion(true);
		
		Job top100=new Job(conf,"Relative Frequency");
		top100.setJarByClass(RelativeFrequency.class);
		FileInputFormat.addInputPath(top100, new Path(args[1]));//output1 & input2
		FileOutputFormat.setOutputPath(top100,new Path(args[2]));//output2 or final output
		top100.setMapperClass(RelativeFrequencyMapper.class);
		top100.setReducerClass(RelativeFrequencyReducer.class);
		top100.setMapOutputKeyClass(FloatWritable.class);
		top100.setMapOutputValueClass(Text.class);
		top100.setOutputKeyClass(LongWritable.class);
		top100.setOutputValueClass(Text.class);
		top100.setSortComparatorClass(FloatWritableComparator.class);
		System.exit(top100.waitForCompletion(true) ? 0 : 1);
		
	}
}
