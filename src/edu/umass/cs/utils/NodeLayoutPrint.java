package edu.umass.cs.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class NodeLayoutPrint 
{
	public static final String FILENAME = "nodeLayout.txt";
	
	public static void main(String[] args)
	{
		BufferedReader br = null;
		FileReader fr = null;
		try 
		{	
			fr = new FileReader(FILENAME);
			br = new BufferedReader(fr);
			
			String sCurrentLine;
			
			br = new BufferedReader(new FileReader(FILENAME));
			String lastMachName = "";
			int numNodesSoFar = 0;
			while ((sCurrentLine = br.readLine()) != null) 
			{
				String[] parsed = sCurrentLine.split(" ");
				//System.out.println(parsed[0].trim()+" "+parsed[1].trim());
				
				String vmName = parsed[0].trim();
				String machineName = vmName.split("-")[0];
				
				if(lastMachName.length() == 0)
				{
					lastMachName = machineName;
					numNodesSoFar++;
				}
				else
				{
					if(lastMachName.equals(machineName))
					{
						numNodesSoFar++;
					}
					else
					{
						System.out.println(numNodesSoFar);
						numNodesSoFar++;
						lastMachName = machineName;
					}
				}
			}
		} catch (IOException e) 
		{
			e.printStackTrace();
		} finally 
		{
			try 
			{
				if (br != null)
					br.close();
				
				if (fr != null)
					fr.close();
				
			} catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
		
	}
}