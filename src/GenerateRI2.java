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

public class GenerateRI2 {

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
	
	
//	if(line.startsWith(","))
//	{
		
	//	String [] keyList = line.split(",");
	
	//	Text newKey = new Text();

	//	for(String k : keyList)
	//	{
	//		if(k=="")
	//		{
	//			continue;
	//		}
	//		if(k.startsWith("KP")||k.startsWith("KC"))
	//		{
	//			newKey.set(k);
	//			word.set(line);
	//			context.write(newKey,word);
	//		}
		
	//	}	

//	}
	String [] attributes = line.split("[ ]+");

	if(attributes.length <2)
	{
		return;
	}

	String [] keyList = attributes[1].split(",");
	
	Text newKey = new Text();

	for(String k : keyList)
	{
		if(k=="")
		{
			continue;
		}
		if(k.startsWith("KP")||k.startsWith("KC"))
		{
			newKey.set(k);
			word.set(line);
			context.write(newKey,word);
		}
		
	}	

    }

    

  }
  
  public static class GenerateRIReducer 
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
      System.err.println("Usage: swin.exp.newconf12.GenerateRI2 <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "hadoop join");
    job.setJarByClass(GenerateRI2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(GenerateRIReducer.class);
    job.setReducerClass(GenerateRIReducer.class);
    
    // job.setPartitionerClass(ICDEPartitioner.class);
    
//    WritableComparator.define(Text.class,new ICDEComparator());

//    job.setSortComparatorClass(ICDEComparator.class);    

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
