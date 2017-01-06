package edu.umass.cs.ubercasestudy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;

public class TaxiQueryIssue
{
	private double minLat						= 8000;
	private double maxLat						= -8000;
	
	private double minLong						= 8000;
	private double maxLong						= -8000;
	
	private final Random randGen;
	
	private final PriorityQueue<TaxiRideInfo> ongoingTaxiRides; 
			
    //new PriorityQueue<String>(10, comparator);
	public TaxiQueryIssue()
	{
		randGen = new Random();
		Comparator<TaxiRideInfo> comparator = new TaxiRideInfo("", -1);
		ongoingTaxiRides = new PriorityQueue<TaxiRideInfo>(10,comparator );
	}
	
	
	public void computeLatLongBounds()
	{
		BufferedReader br = null;
		
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader(new FileReader(Driver.ONE_DAY_TRACE_PATH));
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				if(sCurrentLine.startsWith("#"))
					continue;
				
				String[] lineParsed = sCurrentLine.split(",");
				String dateTime = lineParsed[1];
				
				String[] dateParsed = dateTime.split(" ");
				String date = dateParsed[0];
				
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
					
				
				if( pickupLat < minLat  )
				{
					minLat = pickupLat;
				}
				
				if( dropOffLat < minLat )
				{
					minLat = dropOffLat;
				}
				
				if( pickupLong < minLong  )
				{
					minLong = pickupLong;
				}
				
				if( dropOffLong < minLong )
				{
					minLong = dropOffLong;
				}
				
				
				if( pickupLat > maxLat  )
				{
					maxLat = pickupLat;
				}
				
				if( dropOffLat > maxLat )
				{
					maxLat = dropOffLat;
				}
				
				if( pickupLong > maxLong  )
				{
					maxLong = pickupLong;
				}
				
				if( dropOffLong > maxLong )
				{
					maxLong = dropOffLong;
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
				
			} catch ( IOException ex )
			{
				ex.printStackTrace();
			}
		}
		
		System.out.println("minLat "+minLat+" maxLat "+maxLat+ " minLong "+minLong
				+" maxLong "+maxLong);
	}
	
	
	public void startIssuingQueries()
	{
		BufferedReader br = null;
		
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader(new FileReader(Driver.ONE_DAY_TRACE_PATH));
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				if(sCurrentLine.startsWith("#"))
					continue;
				
				double random = randGen.nextDouble();
				
				if(!(random <= Driver.REQUEST_ISSUE_PROB))
					continue;
				
				try 
				{
					String[] lineParsed = sCurrentLine.split(",");
					String pickDateTimeString = lineParsed[Driver.PICKUP_DATETIME_INDEX-1];
					String dropDateTimeString = lineParsed[Driver.DROPOFF_DATETIME_INDEX-1];
					
					long pickupTime = Driver.dfm.parse(pickDateTimeString).getTime()/1000;
					long dropOffTime = Driver.dfm.parse(dropDateTimeString).getTime()/1000;
					
					
					
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
					
					
					if( pickupLat < minLat  )
					{
						minLat = pickupLat;
					}
					
					if( dropOffLat < minLat )
					{
						minLat = dropOffLat;
					}
					
					if( pickupLong < minLong  )
					{
						minLong = pickupLong;
					}
					
					if( dropOffLong < minLong )
					{
						minLong = dropOffLong;
					}
					
					
					if( pickupLat > maxLat  )
					{
						maxLat = pickupLat;
					}
					
					if( dropOffLat > maxLat )
					{
						maxLat = dropOffLat;
					}
					
					if( pickupLong > maxLong  )
					{
						maxLong = pickupLong;
					}
					
					if( dropOffLong > maxLong )
					{
						maxLong = dropOffLong;
					}
				
				} 
				catch (ParseException e) 
				{
					e.printStackTrace();
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
				
			} catch ( IOException ex )
			{
				ex.printStackTrace();
			}
		}
	}
	
	
	private void sendOutTaxiRequest(double pickupLat, double pickupLong, 
						double dropOffLat, double dropOffLong)
	{
		double latMin = Math.max(pickupLat - Driver.SEARCH_AREA_RANGE, Driver.MIN_LAT);
		double latMax = Math.min(pickupLat+Driver.SEARCH_AREA_RANGE, Driver.MAX_LAT);
		
		double longMin = Math.max(pickupLong - Driver.SEARCH_AREA_RANGE, Driver.MIN_LONG);
		double longMax = Math.min(pickupLong+Driver.SEARCH_AREA_RANGE, Driver.MAX_LONG);
		
		String searchQuery = Driver.LAT_ATTR;
		
	}
	
	public static void main( String[] args )
	{
		TaxiQueryIssue taxObj = new TaxiQueryIssue();
		taxObj.computeLatLongBounds();
	}
}