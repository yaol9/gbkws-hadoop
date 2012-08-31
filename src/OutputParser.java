package test.icde12;
import java.io.IOException;

import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

public class OutputParser {



public static void main(String[] args) throws Exception {
    	       
	
    	try{
	for(int i=0;i<8;i++)
	{
		HashMap<String,Integer> dimensionCount = new HashMap<String,Integer>();

                Path pt=new Path("/user/yao/output/part-r-0000"+i);
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));

		Path ptw=new Path("/user/yao/input2/part-r-0000"+i);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(ptw, true)));
       		
                String line;
                line=br.readLine();
                while (line != null){
			if(line.startsWith("L"))
			{
				String [] keyIdS = line.substring(1).split("[+]");
				if(dimensionCount.containsKey(keyIdS[0])) //O
				{
					dimensionCount.put(keyIdS[0],dimensionCount.get(keyIdS[0])+1);
				}
				else
				{
					dimensionCount.put(keyIdS[0],1);
				}
				if(dimensionCount.containsKey(keyIdS[1])) //P
				{
					dimensionCount.put(keyIdS[1],dimensionCount.get(keyIdS[1])+1);
				}
				else
				{
					dimensionCount.put(keyIdS[1],1);
				}
				if(dimensionCount.containsKey(keyIdS[2])) //S
				{
					dimensionCount.put(keyIdS[2],dimensionCount.get(keyIdS[2])+1);
				}
				else
				{
					dimensionCount.put(keyIdS[2],1);
				}
			}
			else if(line.startsWith("O") || line.startsWith("P") || line.startsWith("S"))
			{
				int space = line.indexOf(" ");
				String keyId = line.substring(0,space-2);
				//System.out.println(keyId);
				if(dimensionCount.containsKey(keyId))
				{
					bw.write(keyId+"+"+dimensionCount.get(keyId)+"+"+line.substring(space,line.length()));
					bw.newLine();
					//System.out.println(keyId+" "+dimensionCount.get(keyId)+" "+line.substring(space,line.length()));
				}
			}
			else
			{
			}
                
         		line=br.readLine();
		        
		 }
		br.close();
	  	bw.close();

	}
         }catch(Exception e){
         }

	



}

}
