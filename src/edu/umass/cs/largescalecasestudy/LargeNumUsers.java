package edu.umass.cs.largescalecasestudy;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.config.ContextServiceConfig.PrivacySchemes;

public class LargeNumUsers
{
	public static final double MIN_US_LAT						= 22.0;
	public static final double MAX_US_LAT						= 48.0;
	public static final double MIN_US_LONG						= -125.0;
	public static final double MAX_US_LONG						= -66.0;
	
	
	public static final String TEXAS_TIMEZONE					= "GMT-6";
	public static final int NUM_EVENT_THRESHOLD					= 10;
	public static final double INSERT_LOSS_TOLERANCE			= 0.0;
	public static final double UPD_LOSS_TOLERANCE				= 0.5;
	
	public  static final String USER_TRACE_DIR					= "/proj/MobilityFirst/ayadavDir/contextServiceScripts/processedIndividualTracesDupRem";
	
	public static final String WEATHER_DATA_PATH 
			//= "/home/ayadav/Documents/Data/NWSWeatherData/wwa_201701010000_201703150000/1Jan15Mar2017Weather.json";
			= "/proj/MobilityFirst/ayadavDir/contextServiceScripts/1Jan15Mar2017Weather.json";
	
	
	public static final String USER_INFO_FILE_PREFIX			= "UserInfo";
	
	// latitude longitude key in json and attribute names in CNS
	public static final String LATITUDE_KEY						= "latitude";
	public static final String LONGITUDE_KEY					= "longitude";
	
	public static final long  TIME_UPDATE_SLEEP_TIME			= 1*1000;  // every minute
	
	public static final long TIME_DIST_INTERVAL					= 60; // 10 minutes on either side.
	
	// 900 s timeslots with highest alert rate
	
	// perdaystore timeslot=2099 timestamp=1485117000 numevents=231 numploygons=244
	// Currently we have results for the first timeslot.
	// perdaystore timeslot=611 timestamp=1483777800 numevents=177 numploygons=245
	// perdaystore timeslot=2088 timestamp=1485107100 numevents=196 numploygons=252
    // perdaystore timeslot=5560 timestamp=1488231900 numevents=51 numploygons=258

	
	public static final long START_UNIX_TIME					= 1483777800;
	
	public static final long END_UNIX_TIME						= 1483778700;
	
	
	//"geoLocationCurrentTimestamp"
	public static final String GEO_LOC_TIME_KEY					= "geoLocationCurrentTimestamp";
	
	
	// "geoLocationCurrent"
	public static final String GEO_LOC_KEY						= "geoLocationCurrent";
	
	//"coordinates"
	public static final String COORD_KEY						= "coordinates";
	
	public static final String GEOLOC_TIME						= "geoLocationCurrentTimestamp";
	
	
	public static final double LAT_LONG_THRESH					= Math.pow(10, -4);
	
	private static String csHost 								= "";
	private static int csPort 									= -1;
	
	public static long numusers;
	
	public static int myID										= 0;
	
	public static double initRate								= 100.0;
	
	public static long currRealUnixTime							= START_UNIX_TIME;
	
	// two users files are written. When UserInfo1 is read then UserInfo2 is written
	// and when 2 is read to perform updates then UserInfo1 is written for next updates.
	public static int userinfoFileNum							= 0;
	
	public static List<String> filenameList;
	public static ContextServiceClient csClient;
	
	public static Random distibutionRand;
	public static String guidFilePath;
	
	// when this flag is set true then search and update
	// requests are executed on mysql locally rather than being sent to cns
	public static boolean localMySQLOper;
	public static int mysqlpoolsize;
	
	public static ExecutorService	 taskES						= null;
	
	public static DataSource dsInst								= null;
	
	public static boolean rateWorkload;
	public static double requestsps;
	public static boolean backTobackReq							= false; 
	
	
	
	public static boolean checkIfRelativeTimeInTimeSlot
								(long relativeTimeFromMidnight) throws ParseException
	{
		Date date = new Date(START_UNIX_TIME*1000);
		// *1000 is to convert seconds to milliseconds
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// the format of your date
		sdf.setTimeZone(TimeZone.getTimeZone(TEXAS_TIMEZONE));
		// give a timezone reference for formating (see comment at the bottom
		String dateFormat = sdf.format(date);
		
		String onlyDate = dateFormat.split(" ")[0];
		
		String midnight = onlyDate+" "+"00:00:00";
		
		Date midnightDate = sdf.parse(midnight);
		
		long midnightTS = midnightDate.getTime()/1000;
		
		long absTS = midnightTS+relativeTimeFromMidnight;
		
		if( (absTS >= START_UNIX_TIME) && (absTS <= END_UNIX_TIME) )
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	
	public static String getSHA1(String stringToHash)
	{
		MessageDigest md = null;
		try
		{
			md = MessageDigest.getInstance("SHA-256");
		} 
		catch (NoSuchAlgorithmException e)
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
	
	public static long computeTimeRelativeToDatStart(long currUnixTimeStamp)
	{
		Date date = new Date(currUnixTimeStamp*1000L); 
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");	
		
		sdf.setTimeZone(TimeZone.getTimeZone(TEXAS_TIMEZONE)); 
		String dateFormat = sdf.format(date);
		
		String onlyDate = dateFormat.split(" ")[0];
		
		try 
		{
			Date midnightDate = sdf.parse(onlyDate+" "+"00:00:00");
			long midnightunixtime = midnightDate.getTime()/1000;
			
			assert(currUnixTimeStamp >= midnightunixtime);
			return (currUnixTimeStamp-midnightunixtime);
		}
		catch (ParseException e)
		{
			e.printStackTrace();
		}
		return -1;
	}
	
	public static class TimerThread implements Runnable
	{
		private long time = 0;
		
		public TimerThread()
		{
		}
		
		@Override
		public void run()
		{
			while(true)
			{
				try
				{
					Thread.sleep((long) TIME_UPDATE_SLEEP_TIME);
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
				
				currRealUnixTime = (long) (currRealUnixTime + (TIME_UPDATE_SLEEP_TIME/1000));
				
				time = time + (long)TIME_UPDATE_SLEEP_TIME;
				//if( (time % 10000) == 0 )
				{
					Date date = new Date((long)currRealUnixTime*1000L); 
					// *1000 is to convert seconds to milliseconds
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
					// the format of your date
					sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
					// give a timezone reference for formating (see comment at the bottom
					String dateFormat = sdf.format(date);
					
					String printStr = "Current date in GMT "+dateFormat;
					
					System.out.println(printStr);
				}
			}
		}
	}
	
	/**
	 * distributes time uniformly in +10 minutes interval of the given time
	 * @return
	 */
	public static long distributeTimeUniformly(long timestamp)
	{
		long diff = distibutionRand.nextInt((int)TIME_DIST_INTERVAL);
		return timestamp+diff;
	}
	
	
	public static void main(String[] args) throws Exception
	{
		numusers 				= Long.parseLong(args[0]);
		csHost 					= args[1];
		csPort 					= Integer.parseInt(args[2]);
		myID        			= Integer.parseInt(args[3]);
		guidFilePath 			= args[4];
		
		
		boolean enableSearch 	= Boolean.parseBoolean(args[5]);
		boolean enableUpdate    = Boolean.parseBoolean(args[6]);
		localMySQLOper          = Boolean.parseBoolean(args[7]);
		rateWorkload         	= Boolean.parseBoolean(args[8]);
		
		if(localMySQLOper)
		{
			mysqlpoolsize = Integer.parseInt(args[9]);
			dsInst = new DataSource();
			taskES = Executors.newFixedThreadPool(mysqlpoolsize);
		}
		
		if(rateWorkload)
		{
			requestsps = Double.parseDouble(args[9]);
			backTobackReq = Boolean.parseBoolean(args[10]);
		}
		
		
		if(!localMySQLOper)
		{
			csClient  	= new ContextServiceClient(csHost, csPort, false, 
				PrivacySchemes.NO_PRIVACY);
		}
		
		if(enableUpdate)
		{
			distibutionRand = new Random((myID+1)*100);
			
			// compute distributions
			DistributionLearningFromTraces.main(null);
						
			filenameList   = new LinkedList<String>();
						
			Iterator<String> filenameIter 
									   = DistributionLearningFromTraces.distributionsMap.keySet().iterator();
						
			while( filenameIter.hasNext() )
			{
				String filename = filenameIter.next();
				filenameList.add(filename);
			}
			
			UserInfoFileWriting userInfoFileWrit = new UserInfoFileWriting();
			userInfoFileWrit.initializeFileWriting();
		}
		
		WeatherBasedSearchQueryIssue searchIssue = null;
		
		if(enableSearch)
		{
			if(myID == 0)
			{
				searchIssue = new WeatherBasedSearchQueryIssue(0.0);
			}
		}
		
		TimerThread timerThread = new TimerThread();
		new Thread(timerThread).start();
		Thread updThread 	= null;
		Thread searchThread = null;
		
		if(enableUpdate)
		{
			if(!rateWorkload)
			{
				try
				{
					TraceBasedUpdate traceBasedUpdate = new TraceBasedUpdate();
					updThread = new Thread(traceBasedUpdate);
					updThread.start();
				}
				catch(Exception | Error ex)
				{
					ex.printStackTrace();
				}
			}
			else
			{
				if(myID == 0)
				{
					RateBasedUpdate rateBased = new RateBasedUpdate();
					updThread = new Thread(rateBased);
					updThread.start();
				}
			}
		}
		
		if(enableSearch)
		{
			if(myID == 0)
			{
				searchThread = new Thread(searchIssue);
				searchThread.start();
			}
		}
		
		if(updThread != null)
		{
			updThread.join();
		}
		
		if(searchThread != null)
		{
			searchThread.join();
		}
		
		System.exit(0);
	}
}