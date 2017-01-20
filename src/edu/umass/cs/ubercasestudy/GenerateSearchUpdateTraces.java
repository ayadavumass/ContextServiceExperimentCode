package edu.umass.cs.ubercasestudy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class GenerateSearchUpdateTraces
{
	public static final double SELEC_PROB 				= 0.025;
	
	public static final String SEARCH_FILE_NAME			= "searchFile.txt";
	public static final String UPDATE_FILE_NAME			= "updateFile.txt";
	
	public static final double SEARCH_AREA_RANGE		= 0.5;
	
	public void writeTrace()
	{
		BufferedWriter searchBW 	= null;
		BufferedWriter updateBW 	= null;
		
		Random randGen 				= new Random();
		
		BufferedReader br 			= null;
		
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader(new FileReader(Driver.USED_TRACE_PATH));
			
			searchBW = new BufferedWriter(new FileWriter(SEARCH_FILE_NAME));
			updateBW = new BufferedWriter(new FileWriter(UPDATE_FILE_NAME));
			
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				if(sCurrentLine.startsWith("#"))
					continue;
				
				double random = randGen.nextDouble();
				
				if(!(random <= SELEC_PROB))
					continue;
				
				
				String[] lineParsed = sCurrentLine.split(",");
					
				// now issue the query.
					
				double pickupLat  
							= Double.parseDouble(lineParsed[Driver.PICKUP_LAT_INDEX-1]);
				double pickupLong 
							= Double.parseDouble(lineParsed[Driver.PICKUP_LONG_INDEX-1]);
					
				double dropOffLat  
							= Double.parseDouble(lineParsed[Driver.DROPOFF_LAT_INDEX-1]);
				double dropOffLong 
							= Double.parseDouble(lineParsed[Driver.DROPOFF_LONG_INDEX-1]);
					
				if( pickupLat == 0.0 || pickupLong == 0.0 || dropOffLat == 0.0 || 
						dropOffLong == 0.0 )
				{
					System.out.println("lineParsed");
				}
				
				String searchQuery = getTaxiSearchQuery(pickupLat, pickupLong, SEARCH_AREA_RANGE);
				
				searchBW.write(searchQuery+"\n");
				
				// >0.5 is in use.
				double inuseRand  = 1-randGen.nextDouble()* Driver.FREE_INUSE_BOUNDARY;
				String updateStr1 = Driver.LAT_ATTR+","+pickupLat+","
								  		+Driver.LONG_ATTR+","+pickupLong;
								  		//+Driver.STATUS_ATTR+","+inuseRand;
				
				updateBW.write(updateStr1+"\n");
				
				double freeRand   = randGen.nextDouble()* Driver.FREE_INUSE_BOUNDARY;
				
				String updateStr2 = Driver.LAT_ATTR+","+dropOffLat+","
				  						+ Driver.LONG_ATTR+","+dropOffLong;
				  						//+ Driver.STATUS_ATTR+","+freeRand;
				
				updateBW.write(updateStr2+"\n");
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				if ( br != null )
					br.close();
				
				if( searchBW != null )
					searchBW.close();
				
				if( updateBW != null )
					updateBW.close();
					
			} catch ( IOException ex )
			{
				ex.printStackTrace();
			}
		}
	}
	
	
	private String getTaxiSearchQuery(double pickupLat, double pickupLong, 
			double searchRange)
	{
		double latMin = Math.max(pickupLat - searchRange, Driver.MIN_LAT);
		double latMax = Math.min(pickupLat + searchRange, Driver.MAX_LAT);

		double longMin = Math.max(pickupLong - searchRange, Driver.MIN_LONG);
		double longMax = Math.min(pickupLong + searchRange, Driver.MAX_LONG);

		String searchQuery = Driver.LAT_ATTR +" >= "+latMin
							+" AND "+Driver.LAT_ATTR+" <= "+latMax
							+" AND "+Driver.LONG_ATTR +" >= "+longMin
							+" AND "+Driver.LONG_ATTR+" <= "+longMax;
							//+" AND "+Driver.STATUS_ATTR+" >= "+Driver.MIN_STATUS
							//+" AND "+Driver.STATUS_ATTR+" <= "+Driver.FREE_INUSE_BOUNDARY;
		return searchQuery;
	}
	
	
	public static void main(String[] args)
	{
		GenerateSearchUpdateTraces obj = new GenerateSearchUpdateTraces();
		obj.writeTrace();
	}
}