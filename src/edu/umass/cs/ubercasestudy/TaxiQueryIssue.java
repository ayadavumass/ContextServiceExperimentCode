package edu.umass.cs.ubercasestudy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class TaxiQueryIssue
{
	public static final int linesToSkip 		= 100;
	
	private double minLat						= 8000;
	private double maxLat						= -8000;
	
	private double minLong						= 8000;
	private double maxLong						= -8000;
	
	
	public TaxiQueryIssue()
	{
		
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
	
	public static void main( String[] args )
	{
		TaxiQueryIssue taxObj = new TaxiQueryIssue();
		taxObj.computeLatLongBounds();
	}
}