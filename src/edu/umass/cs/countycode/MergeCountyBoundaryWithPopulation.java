package edu.umass.cs.countycode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;


public class MergeCountyBoundaryWithPopulation 
{
	public static final String BOUNDS_FILE				
			= "/home/ayadav/Documents/Code/ContextServiceExperiments/processedCountyData.csv";
	public static final String POP_FILE					
			= "/home/ayadav/Documents/Code/ContextServiceExperiments/processedPopulation.csv";

	private static HashMap<String, Long> populationHashMap;
	
	public static void main(String[] args)
	{
		populationHashMap = new HashMap<String, Long>();
				
		BufferedReader boundfile = null;
		BufferedReader popfile   = null;
		
		BufferedWriter bw = null;
		
		try
		{
			String sCurrentLine;
			popfile = new BufferedReader(new FileReader(POP_FILE));
			
			while ((sCurrentLine = popfile.readLine()) != null) 
			{
				String[] parsed = sCurrentLine.split(",");
				
				int statefp = Integer.parseInt(parsed[0]);
				int countyfp = Integer.parseInt(parsed[1]);
				long totpop = Long.parseLong(parsed[2]);
				
				String key = statefp+"-"+countyfp;
				
				populationHashMap.put(key, totpop);
			}
			
		} catch (IOException e) 
		{
			e.printStackTrace();
		} finally 
		{
			try 
			{
				if (popfile != null)
					popfile.close();				
			} catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
		
		try
		{
			bw = new BufferedWriter(new FileWriter("countyData.csv"));
			boundfile = new BufferedReader(new FileReader(BOUNDS_FILE));
			
			String sCurrentLine;
			
			while ((sCurrentLine = boundfile.readLine()) != null) 
			{
				String[] parsed = sCurrentLine.split(",");
				
				int statefp = Integer.parseInt(parsed[0]);
				int countyfp = Integer.parseInt(parsed[1]);
				String countyname = parsed[2];
				String minLat = parsed[3];
				String minLong = parsed[4];
				String maxLat = parsed[5];
				String maxLong = parsed[6];
				
				String key = statefp+"-"+countyfp;
				Long totpop = populationHashMap.get(key);
				
				if(totpop == null)
				{
					String str = statefp+","+countyfp+","+countyname
							+","+minLat+","+minLong+","+maxLat+","+maxLong
							+","+"\n";
					bw.write(str);
				}
				else
				{
					String str = statefp+","+countyfp+","+countyname
									+","+minLat+","+minLong+","+maxLat+","+maxLong
									+","+totpop+"\n";
					bw.write(str);
				}
			}
			
		} catch (IOException e) 
		{
			e.printStackTrace();
		} finally 
		{
			try 
			{
				if (boundfile != null)
					boundfile.close();
				
				if(bw != null)
					bw.close();
				
			} catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
		
	}
}