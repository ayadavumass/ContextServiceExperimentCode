package edu.umass.cs.countycode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class CountyPopulationProcessing 
{
	public static final String COUNTY_POPULATION_FILE 		
				= "/home/ayadav/Documents/Data/CountyPopulation/cc-est2015-alldata.csv";
	
	
	public static final String PROCESSED_COUNTY_POPULATION = "processedPopulation.csv";
	
	
	public static final int STATEFP_INDEX				   = 1;
	public static final int COUNTYFP_INDEX				   = 2;
	public static final int YEAR_INDEX				   	   = 5;
	public static final int AGEGRP_INDEX				   = 6;
	public static final int TOTPOP_INDEX				   = 7;
	
	
	private static void processCountyPopulation()
	{
		BufferedReader br = null;
		BufferedWriter bw = null;
		long totalUSPop = 0;
		try
		{
			String sCurrentLine;
			br = new BufferedReader(new FileReader(COUNTY_POPULATION_FILE));
			bw = new BufferedWriter(new FileWriter(PROCESSED_COUNTY_POPULATION));
			
			// skipping first line
			br.readLine();
			
			while ( (sCurrentLine = br.readLine()) != null )
			{
				String[] parsed = sCurrentLine.split(",");
				int statefp = Integer.parseInt(parsed[STATEFP_INDEX]);
				int countyfp = Integer.parseInt(parsed[COUNTYFP_INDEX]);
				int yearcode = Integer.parseInt(parsed[YEAR_INDEX]);
				int agegrpcode = Integer.parseInt(parsed[AGEGRP_INDEX]);
				long totpop = Long.parseLong(parsed[TOTPOP_INDEX]);
				
				// 8 is 2015 year code in the data, 0 is the age code for all ages
				if( (yearcode == 8) && (agegrpcode == 0) )
				{
					String str = statefp+","+countyfp+","+totpop+"\n";
					totalUSPop = totalUSPop + totpop;
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
				if (br != null)
					br.close();
				
				if( bw != null)
					bw.close();
			} catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
		
		System.out.println("Total US pop in 2015 "+totalUSPop);
	}
	
	
	public static void main(String[] args)
	{
		processCountyPopulation();	
	}
}