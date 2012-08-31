package swin.exp.newconf12;
import java.io.IOException;
import java.util.StringTokenizer;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.WritableComparator;

import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import swin.utils.Helper;

public class GenerateRI {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
    

	private Text word = new Text();
      
//	private String k0=""; //keyword on Order
//	private String k1=""; //keyword on Part
//	private String k2=""; //keyword on Supplier

/*
	protected void setup(Context context
                          ) throws IOException, InterruptedException {
		super.setup(context); 
		Path pt=new Path("/user/yao/query/query");
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line=br.readLine();
		String [] keywords = line.split(",");
		k0=keywords[0];		
		k1=keywords[1];
		k2=keywords[2];
		br.close();
	}
*/ 
   public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

	String line = value.toString();

	String [] attributes = line.split("[|]");

	String tableName = Helper.getTableName(line);

	String delims = "[ .,?!:;\\-]+";
	
	if(tableName.equalsIgnoreCase("lineitem"))
	{

		String[] tokens = attributes[13].split(delims);
		
		String v = "KO"+attributes[0]+",KP"+attributes[1]+",KS"+attributes[2];
		for(String s:tokens)
		{
			v=v+","+s;
		}
		word.set(v);

		String keyS = "KO"+attributes[0];
		Text newKey = new Text(keyS);
		context.write(newKey, word);

		keyS ="KP"+attributes[1];
		newKey.set(keyS);
		context.write(newKey,word);
		
		keyS ="KS"+attributes[2];
		newKey.set(keyS);
		context.write(newKey,word);

	}
	else if(tableName.equalsIgnoreCase("supplier"))
	{
		String[] tokens = attributes[6].split(delims);
		
		String v = "KS"+attributes[0]+",KN"+attributes[3];
		for(String s:tokens)
		{
			v=v+","+s;
		}
		word.set(v);

		String keyS = "KS"+attributes[0];
		Text newKey = new Text(keyS);
		context.write(newKey, word);

		keyS ="KN"+attributes[3];
		newKey.set(keyS);
		context.write(newKey,word);
			
	}
	else if(tableName.equalsIgnoreCase("part"))
	{
		String[] tokens = attributes[8].split(delims);
		
		String v = "KP"+attributes[0];
		for(String s:tokens)
		{
			v=v+","+s;
		}
		word.set(v);
		
		String keyS = "KP"+attributes[0];
		Text newKey = new Text(keyS);
		context.write(newKey, word);
		
	}
	else if(tableName.equalsIgnoreCase("order"))
	{
		String[] tokens = attributes[8].split(delims);
		
		String v = "KO"+attributes[0]+",KC"+attributes[1];
		for(String s:tokens)
		{
			v=v+","+s;
		}
		word.set(v);
		
		String keyS = "KO"+attributes[0];
		Text newKey = new Text(keyS);
		context.write(newKey, word);

		keyS ="KC"+attributes[1];
		newKey.set(keyS);
		context.write(newKey,word);
	}
	else if(tableName.equalsIgnoreCase("nation"))
	{
		String[] tokens = (attributes[1]+","+attributes[3]).split(delims);
		
		String v = "KN"+attributes[0]+",KR"+attributes[2];
		for(String s:tokens)
		{
			v=v+","+s;
		}
		word.set(v);
		
		String keyS = "KN"+attributes[0];
		Text newKey = new Text(keyS);
		context.write(newKey, word);

		keyS ="KR"+attributes[2];
		newKey.set(keyS);
		context.write(newKey,word);
	}
	else if(tableName.equalsIgnoreCase("region"))
	{
		String[] tokens = (attributes[1]+","+attributes[2]).split(delims);

		String v = "KR"+attributes[0];
		for(String s:tokens)
		{
			v=v+","+s;
		}
		word.set(v);
		
		String keyS = "KR"+attributes[0];
		Text newKey = new Text(keyS);
	}
	else if(tableName.equalsIgnoreCase("customer"))
	{
		String[] tokens = attributes[7].split(delims);
		
		String v = "KC"+attributes[0]+",KN"+attributes[3];
		for(String s:tokens)
		{
			v=v+","+s;
		}
		word.set(v);
		
		String keyS = "KC"+attributes[0];
		Text newKey = new Text(keyS);
		context.write(newKey, word);

		keyS ="KN"+attributes[3];
		newKey.set(keyS);
		context.write(newKey,word);
	}
	else
	{
	}

    }

    

  }
  
  public static class IntSumReducer 
       extends Reducer<Text,Text,Text,Text> {
	private Text result = new Text();
	//private HashMap<String,Integer> dimensionData;
    
   	//
//	protected void setup(Context context
  //                        ) throws IOException, InterruptedException {
	//	super.setup(context);
	//	dimensionData = new HashMap<String,Integer>();
		
		
	//}

	public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
	
	List<String> keyList = new ArrayList<String>();
	List<String> wordList = new ArrayList<String>();

	for(Text value: values)
	{
		String vS = value.toString();
		String[] tokens = vS.split(",");
		for(String t:tokens)
		{
			if(t.startsWith("KO") ||t.startsWith("KP") || t.startsWith("KS"))
			{
				if(!keyList.contains(t))
				{				
					keyList.add(t);	
				}
			}
			else
			{
				
				if(!wordList.contains(t))
				{				
					wordList.add(t);	
				}
				
			}
		}
	}

	//output
	String outS="";
	for(String k:keyList)
	{	
		outS+=","+k;
	}

	for(String w:wordList)
	{	
		outS+=","+w;
	}

	result.set(outS);
	context.write(key,result);		
      
    }
  }



 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: swin.exp.newconf12.GenerateRI <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "hadoop join");
    job.setJarByClass(GenerateRI.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    
    // job.setPartitionerClass(ICDEPartitioner.class);
    
//    WritableComparator.define(Text.class,new ICDEComparator());

//    job.setSortComparatorClass(ICDEComparator.class);    

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(8);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
