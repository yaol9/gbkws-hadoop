package swin.utils;

public class Helper {

  public static String getTableName(String line)
    {
	String returnVal ="";
	String [] attributes = line.split("[|]");
        int attributeSize = attributes.length;

	if(attributeSize>10) //lineitem table
	{
	   returnVal = "lineitem";
	}
	else if (attributeSize==3)
	{
	   returnVal = "region";
	}
	else if (attributeSize==4)
	{
	   returnVal = "nation";
	}
	
	else if (attributeSize==8)
	{
	   returnVal = "customer";
	}

	else if (attributeSize<8 && attributeSize>4) //supply
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
