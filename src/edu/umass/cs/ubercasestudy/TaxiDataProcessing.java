package edu.umass.cs.ubercasestudy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
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
	
	public void readTheTaxiFile() throws ParseException
	{
		BufferedReader br = null;
		BufferedWriter bw = null;
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader(new FileReader(Driver.TAXI_DATA_PATH));
			bw = new BufferedWriter(new FileWriter(Driver.USED_TRACE_PATH));
			// skip first line
			br.readLine();
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				
				String[] lineParsed = sCurrentLine.split(",");
				String pickupDateTimeString = lineParsed[Driver.PICKUP_DATETIME_INDEX-1];
				//String dropOffDateTimeString = lineParsed[Driver.DROPOFF_DATETIME_INDEX-1];
				
				Date pickUpDate = Driver.dfm.parse(pickupDateTimeString);
				//Date dropOffDate = Driver.dfm.parse(dropOffDateTimeString);
				
				String[] pickUpDateParsed = pickupDateTimeString.split(" ");
				//String[] dropOffDateParsed = dropOffDateTimeString.split(" ");
				
				String pickUpDateString = pickUpDateParsed[0];
				//String dropOffDateString = dropOffDateParsed[0];
				
				
				if( perDayTaxiPickUps.containsKey(pickUpDateString) )
				{
					long currNum = perDayTaxiPickUps.get(pickUpDateString);
					currNum++;
					perDayTaxiPickUps.put(pickUpDateString, currNum);
				}
				else
				{
					perDayTaxiPickUps.put(pickUpDateString, new Long(1));
				}
				
				if( pickUpDate.getDate() >= Driver.MIN_DATE 
								&& pickUpDate.getDate() <= Driver.MAX_DATE )
				{
					if( filterDataPoints(sCurrentLine) )
					{
						bw.write(sCurrentLine+","+
								pickUpDate.getTime()+"\n");
					}
				}
				
//				if( pickUpDate.equals(dropOffDate) )
//				{
//					if( pickUpDate.equals(Driver.DATE_TO_SAMPLE) )
//					{
//						if( filterDataPoints(sCurrentLine) )
//						{
//							bw.write(sCurrentLine+","+
//										Driver.dfm.parse(pickupDateTime).getTime()+"\n");
//						}
//					}
//				}
				
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
	
	
	public static void main(String[] args) throws ParseException
	{
		TaxiDataProcessing taxiData = new TaxiDataProcessing();
		taxiData.readTheTaxiFile();	
	}
}