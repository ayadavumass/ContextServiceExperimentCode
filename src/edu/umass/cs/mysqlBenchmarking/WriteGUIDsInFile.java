package edu.umass.cs.mysqlBenchmarking;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class WriteGUIDsInFile
{
	public static final String ATTR_NAME_PREFIX				= "attr";
	public static final String GUID_PREFIX					= "guid";
	public static final double MIN_ATTR_VAL					= 1.0;
	public static final double MAX_ATTR_VAL					= 1500.0;
	
	
	
	public static void main(String[] args)
	{
		long numberGuids = Long.parseLong(args[0]);
		int numAttrs	 = Integer.parseInt(args[1]);
		
		Random rand = new Random(100);
		
		
		BufferedWriter bw = null;
		try
		{
			bw = new BufferedWriter(new FileWriter("guidsInfoFile.txt"));
			
			long currGuidNum = 0;
			
			while(currGuidNum < numberGuids)
			{
				String str = "";
				String guid = getSHA1(GUID_PREFIX+currGuidNum);
				//str = "X'"+guid+"'";
				str = guid;
				currGuidNum++;
				
				for(int i=0; i< numAttrs; i++)
				{
					String attrName = ATTR_NAME_PREFIX+i;
					double val = MAX_ATTR_VAL*rand.nextDouble();
					str = str+","+val;
				}
				bw.write(str+"\n");
			}
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
		}
		finally
		{
			if(bw != null)
			{
				try 
				{
					bw.close();
				} 
				catch (IOException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	public static String getSHA1(String stringToHash)
	{
		MessageDigest md = null;
		try
		{
			md = MessageDigest.getInstance("SHA-256");
		} 
		catch (NoSuchAlgorithmException e)
		{
			e.printStackTrace();
		}
		
		md.update(stringToHash.getBytes());
		
		byte byteData[] = md.digest();
       
		//convert the byte to hex format method 1
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < byteData.length; i++) 
        {
        	sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
        }
        String returnGUID = sb.toString();
        return returnGUID.substring(0, 40);
	}
}