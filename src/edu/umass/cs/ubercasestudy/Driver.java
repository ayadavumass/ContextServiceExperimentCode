package edu.umass.cs.ubercasestudy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.config.ContextServiceConfig.PrivacySchemes;

public class Driver
{
	public static final double LOSS_TOLERANCE				= 0.5;
	public static final double WAIT_TIME					= 300000000; // 100 sec
	
	// this is approximately similar to taxis in nyc, which is around 13000.
	public static int NUMBER_TAXIS							= 100;
	
	// indexes start from 1. so In the parsed array they should be 
	// made -1
	public static final int PICKUP_LAT_INDEX				= 7;
	public static final int PICKUP_LONG_INDEX				= 6;
	public static final int DROPOFF_LAT_INDEX				= 11;
	public static final int DROPOFF_LONG_INDEX				= 10;
	public static final int PICKUP_DATETIME_INDEX			= 2;
	public static final int DROPOFF_DATETIME_INDEX			= 3;
	
	
	//NYC lat 40.7128, long -74.0059
	public static final double MIN_LAT						= 40.0;
	public static final double MAX_LAT						= 42.0;
	
	public static final double MIN_LONG						= -76.0;
	public static final double MAX_LONG						= -73.0;
	
	public static final double MIN_STATUS					= 0.0;
	public static final double MAX_STATUS					= 1.0;
	
	
//	public static   String ONE_DAY_TRACE_PATH
//					= "/home/ayadav/Documents/Data/NYCTaxiData/13FebTrace.csv";
	
	public static   String USED_TRACE_PATH
					= "/home/ayadav/Documents/Data/NYCTaxiData/1WeekTrace.csv";
	
	public static final String TAXI_DATA_PATH 
					= "/home/ayadav/Documents/Data/NYCTaxiData/yellow_tripdata_2016-02.csv";
	
	//public static final String DATE_TO_SAMPLE 				= "2016-02-13";
	
	// from 1st Feb
	public static final int MIN_DATE 						= 1;
	// 14th Feb
	public static final int MAX_DATE 						= 4;
	
	public static final String LAT_ATTR						= "latitude";
	public static final String LONG_ATTR					= "longitude";
	public static final String STATUS_ATTR					= "status";
	
//	public static final double FREE_TAXI_STATUS				= 0.5;
//	public static final double INUSE_TAXI_STATUS			= 1.5;
	
	// < 0.5 free, >= 0.5 in use
	public static final double FREE_INUSE_BOUNDARY			= 0.5;
	
	public static final String GUID_PREFIX					= "TAXIGUID";
	
	public static final DateFormat dfm 						= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static double startUnixTimeInSec					= -1;
	public static double currUnixTimeInSec					= -1;
	public static ContextServiceClient csClient;
	
	public static final double SLEEP_TIME					= 100; //100ms;
	public static double TIME_CONTRACTION_FACTOR			= 720.0; // 96 means running 1 day trace in 15 mins
	
	// 0.5 means 50% of trace will be sent. so 50% users got taxis.
	public static double REQUEST_ISSUE_PROB					= 0.5;
	
	// 0.2 on each side, like search at 40 would be from 38.8 to 40.2
	public static double SEARCH_AREA_RANGE					= 0.2;
	
	public static int myID;
	
	// request sender waits 
	public static final Object TIME_WAIT_LOCK				= new Object();
	
	// key is taxi GUID, Boolean is true if taxi is free , false if not.
	public static final HashMap<String, Boolean> taxiFreeMap		
															= new HashMap<String, Boolean>();
	
	
	public static ExecutorService execServ;;
	
	public static TaxiQueryIssue tqi;
	
	public static void main(String[] args) throws NoSuchAlgorithmException, IOException, ParseException
	{
		String csHost = args[0];
		int csPort = Integer.parseInt(args[1]);
		NUMBER_TAXIS = Integer.parseInt(args[2]);
		TIME_CONTRACTION_FACTOR = Double.parseDouble(args[3]);
		REQUEST_ISSUE_PROB = Double.parseDouble(args[4]);
		SEARCH_AREA_RANGE = Double.parseDouble(args[5]);
		int numThreads = Integer.parseInt(args[6]);
		Driver.myID = Integer.parseInt(args[7]);
		USED_TRACE_PATH = args[8];
		
		
		
		execServ = Executors.newFixedThreadPool(numThreads);
		
		System.out.println("Input parameters NUMBER_TAXIS="+NUMBER_TAXIS
				+" TIME_CONTRACTION_FACTOR="+TIME_CONTRACTION_FACTOR
				+" REQUEST_ISSUE_PROB="+REQUEST_ISSUE_PROB
				+" SEARCH_AREA_RANGE="+SEARCH_AREA_RANGE
				+" numThreads="+numThreads
				+" ONE_DAY_TRACE_PATH="+USED_TRACE_PATH);
		
		
		long startUnixTimeInSec = findMinimumTimeFromTrace();
		currUnixTimeInSec = startUnixTimeInSec;
		System.out.println("minTime "+startUnixTimeInSec+new Date(startUnixTimeInSec*1000));
		
		
		csClient = new ContextServiceClient(csHost, csPort, false, 
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
		
		ClockThread clkThred = new ClockThread();
		new Thread(clkThred).start();
		
		
		tqi = new TaxiQueryIssue();
		tqi.startIssuingQueries();
		
		System.out.println("Experiment complete");
		System.exit(0);
	}
	
	public static double getCurrUnixTime()
	{
		return currUnixTimeInSec;
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
	
	/**
	 * returns time in unix timestamp.
	 * @return
	 * @throws ParseException 
	 */
	public static long findMinimumTimeFromTrace() throws ParseException
	{
		long minTime = -1;
		long maxTime = -1;
		BufferedReader br = null;
		try
		{
			String sCurrentLine;
			br = new BufferedReader(new FileReader(Driver.USED_TRACE_PATH));
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				String[] lineParsed = sCurrentLine.split(",");
				String pickupDateTime = lineParsed[Driver.PICKUP_DATETIME_INDEX-1];
				String dropOffDateTime = lineParsed[Driver.DROPOFF_DATETIME_INDEX-1];
				
				long pickupUnixTime = dfm.parse(pickupDateTime).getTime()/1000;
				long dropOffUnixTime = dfm.parse(dropOffDateTime).getTime()/1000;
				
				assert(pickupUnixTime <= dropOffUnixTime);
				
				if(minTime == -1)
				{
					minTime = pickupUnixTime;
				}
				else if(pickupUnixTime < minTime)
				{
					minTime = pickupUnixTime;
				}
				
				if(dropOffUnixTime > maxTime)
					maxTime = dropOffUnixTime;
				
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
			}
			catch ( IOException ex )
			{
				ex.printStackTrace();
			}
		}
		System.out.println("max time "+maxTime+" "+new Date(maxTime*1000));
		return minTime;
	}
	
	public static class ClockThread implements Runnable
	{	
		@Override
		public void run()
		{
			double printSum = 0;
			while(true)
			{
				try 
				{
					Thread.sleep((int)SLEEP_TIME);
					printSum = printSum + SLEEP_TIME;
				} 
				catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
				
				double increment = (SLEEP_TIME/1000.0)*TIME_CONTRACTION_FACTOR;
				currUnixTimeInSec = currUnixTimeInSec + increment;	
				
				synchronized(TIME_WAIT_LOCK)
				{
					TIME_WAIT_LOCK.notifyAll();
				}
				if(printSum%(50*SLEEP_TIME) == 0)
				{
					printSum = 0;
					System.out.println("Curr time "+new Date((long) (currUnixTimeInSec*1000))
							+ " numSent "+tqi.numSent+" numRecvd "+tqi.numRecvd
							+ " numSearchSent"+tqi.numSearchSent+" numSearchRecvd "+tqi.numSearchRecvd
							+ " numUpdateSent "+tqi.numUpdateSent+" numUpdateRecvd "+tqi.numUpdateRecvd);
				}
			}
		}
	}
	
}