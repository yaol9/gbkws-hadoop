package test.icde12;
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



public class HadoopJoin {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
    

	private Text word = new Text();
      
	private String k0=""; //keyword on Order
	private String k1=""; //keyword on Part
	private String k2=""; //keyword on Supplier

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
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

	String line = value.toString();

	String [] attributes = line.split("[|]");

	String tableName = getTableName(line);

	if(tableName.equalsIgnoreCase("lineitem"))
	{
		word.set("LO"+attributes[0]+"+P"+attributes[1]+"+S"+attributes[2]); //orderkey+partkey+supplykey
		Text v = new Text(" ");
		context.write(word, v);
	}
	else if(tableName.equalsIgnoreCase("supplier"))
	{
		if(line.contains(k2))
		{
		Text v = new Text(attributes[6]);
		word.set("S"+attributes[0]+"A");		
       		context.write(word, v);
		word.set("S"+attributes[0]+"B");		
       		context.write(word, v);
		word.set("S"+attributes[0]+"C");		
       		context.write(word, v);
		word.set("S"+attributes[0]+"D");		
       		context.write(word, v);	
		}	
	}
	else if(tableName.equalsIgnoreCase("part"))
	{
		if(line.contains(k1))
		{
		Text v = new Text(attributes[1]+" "+attributes[4]+" "+attributes[6]+" "+attributes[8]);
		word.set("P"+attributes[0]+"A");		
       		context.write(word, v);
		word.set("P"+attributes[0]+"B");		
       		context.write(word, v);
		word.set("P"+attributes[0]+"C");		
       		context.write(word, v);
		word.set("P"+attributes[0]+"D");		
       		context.write(word, v);
		}
	}
	else if(tableName.equalsIgnoreCase("order"))
	{
		if(line.contains(k0))
		{
		Text v = new Text(attributes[8]);
		word.set("O"+attributes[0]+"A");		
       		context.write(word, v);
		word.set("O"+attributes[0]+"B");		
       		context.write(word, v);
		word.set("O"+attributes[0]+"C");		
       		context.write(word, v);
		word.set("O"+attributes[0]+"D");		
       		context.write(word, v);
		}
	}
	else
	{
	}

    }

    private String getTableName(String line)
    {
	String returnVal ="";
	String [] attributes = line.split("[|]");
        int attributeSize = attributes.length;

	if(attributeSize>10) //lineitem table
	{
	   returnVal = "lineitem";
	}
	else if (attributeSize<8) //supply
	{
	   returnVal = "supplier";
	}
	else 
	{
	   if(attributes[2].length()>2) //part
	   {
		returnVal = "part";
	   }
	   else
	   {
		returnVal = "order";
	   }	
	} 

	return returnVal;
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

      	String keyS = key.toString();
	if(keyS.startsWith("O") || keyS.startsWith("P") || keyS.startsWith("S"))
	{
		String sum = new String();
		
		for (Text val : values)
		{
			
		        sum+=(" "+val.toString());
 	 	}
		
		//String subKey = keyS.substring(0,keyS.length()-1);
		
		//Text t = new Text();
		//t.set(subKey);	
		result.set(sum);	
		context.write(key,result);		
	
	}
	if(keyS.startsWith("L"))
	{
	//	String [] keyIdS = keyS.substring(1).split("[+]");
		
		result.set(" ");
		context.write(key, result);
		
		//String KeyIdS1 = keyIdS[1];
		//result.set(KeyIdS1);
		//context.write(key, result);

		//String KeyIdS2 = keyIdS[2];
		//result.set(KeyIdS2);
		//context.write(key, result);
	

	}

      
    }
  }

   public static class ICDEPartitioner<K,V> extends Partitioner<K,V> {
	 	//O: 1-{4,5,6,7} 0-{0,1,2,3}
		//P: 1-{2,3,6,7} 0-{0,1,4,5}
		//S: 1-{1,3,5,7} 0-{0,2,4,6}
	    public int getPartition(K key, V value, int numReduceTasks) {
		
		String line = key.toString();
		

		if(line.startsWith("O")) //order
		{	
			String keyIdS = line.substring(1,line.length()-1);
			String placeId = line.substring(line.length()-1);
			Integer keyId=0;
			
			try{
				keyId = Integer.parseInt( keyIdS);
			}
			catch(Exception ex)
			{
				return 0;
			}
			if(keyId % 2 == 1)
			{
				//4,5,6,7
				if(placeId.equalsIgnoreCase("A"))
				{
					return 4;
				}
				else if(placeId.equalsIgnoreCase("B"))
				{
					return 5;
				}
				else if(placeId.equalsIgnoreCase("C"))
				{
					return 6;
				}
				else if(placeId.equalsIgnoreCase("D"))
				{
					return 7;
				}
				else
				{
					return 0;
				}
			}
			else
			{
				//0,1,2,3
				if(placeId.equalsIgnoreCase("A"))
				{
					return 0;
				}
				else if(placeId.equalsIgnoreCase("B"))
				{
					return 1;
				}
				else if(placeId.equalsIgnoreCase("C"))
				{
					return 2;
				}
				else if(placeId.equalsIgnoreCase("D"))
				{
					return 3;
				}
				else
				{		
					return 0;
				}
			}
		}	
		else if(line.startsWith("P")) //part
		{	
			String keyIdS = line.substring(1,line.length()-1);
			String placeId = line.substring(line.length()-1);
			Integer keyId=0;
			
			try{
				keyId = Integer.parseInt( keyIdS);
			}
			catch(Exception ex)
			{
				return 0;
			}
				
			if(keyId % 2 == 1)
			{
				//2,3,6,7
				if(placeId.equalsIgnoreCase("A"))
				{
					return 2;
				}
				else if(placeId.equalsIgnoreCase("B"))
				{
					return 3;
				}
				else if(placeId.equalsIgnoreCase("C"))
				{
					return 6;
				}
				else if(placeId.equalsIgnoreCase("D"))
				{
					return 7;
				}
				else
				{
					return 0;
				}
			}
			else
			{
				//0,1,4,5
				if(placeId.equalsIgnoreCase("A"))
				{
					return 0;
				}
				else if(placeId.equalsIgnoreCase("B"))
				{
					return 1;
				}
				else if(placeId.equalsIgnoreCase("C"))
				{
					return 4;
				}
				else if(placeId.equalsIgnoreCase("D"))
				{
					return 5;
				}
				else
				{		
					return 0;
				}
			}
		}
		else if(line.startsWith("S")) //supplier
		{
			String keyIdS = line.substring(1,line.length()-1);
			String placeId = line.substring(line.length()-1);
			Integer keyId=0;
			
			try{
				keyId = Integer.parseInt( keyIdS);
			}
			catch(Exception ex)
			{
				return 0;
			}
					
			if(keyId % 2 == 1)
			{
				//1,3,5,7
				if(placeId.equalsIgnoreCase("A"))
				{
					return 1;
				}
				else if(placeId.equalsIgnoreCase("B"))
				{
					return 3;
				}
				else if(placeId.equalsIgnoreCase("C"))
				{
					return 5;
				}
				else if(placeId.equalsIgnoreCase("D"))
				{
					return 7;
				}
				else
				{
					return 0;
				}
			}
			else
			{
				//0,2,4,6
				if(placeId.equalsIgnoreCase("A"))
				{
					return 0;
				}
				else if(placeId.equalsIgnoreCase("B"))
				{
					return 2;
				}
				else if(placeId.equalsIgnoreCase("C"))
				{
					return 4;
				}
				else if(placeId.equalsIgnoreCase("D"))
				{
					return 6;
				}
				else
				{		
					return 0;
				}
			}
		}
		else if(line.startsWith("L")) //lineitem
		{
			String [] keyIdS = line.substring(1).split("[+]");
									
			Integer keyId0=0;
			Integer keyId1=0;
			Integer keyId2=0;
			
			try{
				keyId0 = Integer.parseInt(  keyIdS[0].substring(1) ) % 2; //Order
				keyId1 = Integer.parseInt(  keyIdS[1].substring(1) ) % 2; //Part
				keyId2 = Integer.parseInt(  keyIdS[2].substring(1) ) % 2; //Supplier
			}
			catch(Exception ex)
			{
				return 0;
			}
			return 4*keyId0+2*keyId1+1*keyId2;
							
		}
		else
		{
			return 0;
		}
	    }
	
	  
}

  public static class ICDEComparator extends WritableComparator {

	protected ICDEComparator()
        {
            super(Text.class, true);
        }
	
	
 	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	
  	
	if(l1>l2)
	{	
		return -1;
	}
	else
	{
		return 1;
	}
	
  }

}
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: test.icde12.HadoopJoin <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "hadoop join");
    job.setJarByClass(HadoopJoin.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    
    job.setPartitionerClass(ICDEPartitioner.class);
    
//    WritableComparator.define(Text.class,new ICDEComparator());

    job.setSortComparatorClass(ICDEComparator.class);    

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(8);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
