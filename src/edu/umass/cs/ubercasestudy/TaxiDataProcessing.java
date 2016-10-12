package edu.umass.cs.ubercasestudy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;


public class TaxiDataProcessing
{	
	private HashMap<String, Long> perDayTaxiPickUps;
	
	private static double minLat;
	private static double maxLat;
	private static double minLong;
	private static double maxLong;
	
	public TaxiDataProcessing()
	{
		//currLineNum = 0;
		perDayTaxiPickUps = new HashMap<String, Long>();
	}
	
	public void processData()
	{
		
	}
	
	public void readTheTaxiFile()
	{
		BufferedReader br = null;
		BufferedWriter bw = null;
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader(new FileReader(Driver.TAXI_DATA_PATH));
			bw = new BufferedWriter(new FileWriter(Driver.ONE_DAY_TRACE_PATH));
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				String[] lineParsed = sCurrentLine.split(",");
				String pickupDateTime = lineParsed[Driver.PICKUP_DATETIME_INDEX-1];
				String dropOffDateTime = lineParsed[Driver.DROPOFF_DATETIME_INDEX-1];
				
				
				String[] pickUpDateParsed = pickupDateTime.split(" ");
				String[] dropOffDateParsed = dropOffDateTime.split(" ");
				
				String pickUpDate = pickUpDateParsed[0];
				String dropOffDate = dropOffDateParsed[0];
				
				
				if( perDayTaxiPickUps.containsKey(pickUpDate) )
				{
					long currNum = perDayTaxiPickUps.get(pickUpDate);
					currNum++;
					perDayTaxiPickUps.put(pickUpDate, currNum);
				}
				else
				{
					perDayTaxiPickUps.put(pickUpDate, new Long(1));
				}
				
				if( pickUpDate.equals(dropOffDate) )
				{
					if( pickUpDate.equals(Driver.DATE_TO_SAMPLE) )
					{
						if( filterDataPoints(sCurrentLine) )
						{
							bw.write(sCurrentLine+"\n");
						}
					}
				}
				
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
				
				if( bw != null )
					bw.close();
				
			} catch ( IOException ex )
			{
				ex.printStackTrace();
			}
		}
		
		Iterator<String> dateIter = perDayTaxiPickUps.keySet().iterator();
		
		while( dateIter.hasNext() )
		{
			String date = dateIter.next();
			
			long totalPickup = perDayTaxiPickUps.get(date);
			System.out.println("Date "+date+" totalPickup "+totalPickup);
		}
	}
	
	
	private boolean filterDataPoints(String logLine)
	{
		String[] lineParsed = logLine.split(",");
		
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
			return false;
		}
		else
		{			
			if(checkLatRange(pickupLat) && checkLatRange(dropOffLat) && 
					checkLongRange(pickupLong) && checkLongRange(dropOffLong) )
			{
				return true;
			}
			else
			{
				return false;
			}
		}
	}
	
	private boolean checkLatRange(double latToCheck)
	{
		if( (latToCheck >= Driver.MIN_LAT) && (latToCheck <= Driver.MAX_LAT) )
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	private boolean checkLongRange(double longToCheck)
	{
		if( (longToCheck >= Driver.MIN_LONG) && (longToCheck <= Driver.MAX_LONG) )
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	
	public static void main(String[] args)
	{
		TaxiDataProcessing taxiData = new TaxiDataProcessing();
		taxiData.readTheTaxiFile();	
	}
}