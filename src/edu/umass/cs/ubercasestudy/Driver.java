package edu.umass.cs.ubercasestudy;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.config.ContextServiceConfig.PrivacySchemes;

public class Driver
{
	// this is approximately similar to taxis in nyc, which is around 13000.
	public static int NUMBER_TAXIS						= 10000;
	
	// indexes start from 1. so In the parsed array they should be 
	// made -1
	public static final int PICKUP_LAT_INDEX			= 7;
	public static final int PICKUP_LONG_INDEX			= 6;
	public static final int DROPOFF_LAT_INDEX			= 11;
	public static final int DROPOFF_LONG_INDEX			= 10;
	public static final int PICKUP_DATETIME_INDEX		= 2;
	public static final int DROPOFF_DATETIME_INDEX		= 3;
	
	
	//NYC lat 40.7128, long -74.0059
	public static final double MIN_LAT					= 40.0;
	public static final double MAX_LAT					= 42.0;
	
	public static final double MIN_LONG					= -76.0;
	public static final double MAX_LONG					= -73.0;
	
	
	public static final String ONE_DAY_TRACE_PATH
					= "/home/adipc/Documents/NYCTaxiData/13FebTrace.csv";
	
	public static final String TAXI_DATA_PATH 
					= "/home/adipc/Documents/NYCTaxiData/yellow_tripdata_2016-02.csv";
	
	public static final String DATE_TO_SAMPLE 			= "2016-02-13";
	
	public static final String LAT_ATTR					= "latitude";
	public static final String LONG_ATTR				= "longitude";
	public static final String STATUS_ATTR				= "status";
	
	public static final int FREE_TAXI_STATUS			= 0;
	public static final int INUSE_TAXI_STATUS			= 10;
	
	public static final String GUID_PREFIX				= "TAXIGUID";
	
	public static final DateFormat dfm 					= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	
	public static ContextServiceClient csClient;
	public static int myID;
	
	public static void main(String[] args) throws NoSuchAlgorithmException, IOException
	{
		String csHost = args[0];
		int csPort = Integer.parseInt(args[1]);
		
		csClient = new ContextServiceClient(csHost, csPort, true, 
				PrivacySchemes.NO_PRIVACY);
		
		InitializeTaxisClass initializeTaxi = new InitializeTaxisClass();
		
		try
		{
			initializeTaxi.initializaRateControlledRequestSender();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	public static String getSHA1(String stringToHash)
	{
		MessageDigest md = null;
		try
		{
			md = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e)
		{
			e.printStackTrace();
		}
       
	   md.update(stringToHash.getBytes());
 
       byte byteData[] = md.digest();
 
       //convert the byte to hex format method 1
       StringBuffer sb = new StringBuffer();
       for (int i = 0; i < byteData.length; i++) 
       {
       		sb.append(Integer.toString
       				((byteData[i] & 0xff) + 0x100, 16).substring(1));
       }
       String returnGUID = sb.toString();
       return returnGUID.substring(0, 40);
	}
	
	public static class ClockThread implements Runnable
	{
		public ClockThread()
		{
		}
		
		@Override
		public void run()
		{
//			while()
//			{
//				
//			}
		}
	}
}