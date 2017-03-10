package edu.umass.cs.largescalecasestudy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.config.ContextServiceConfig.PrivacySchemes;


public class LargeNumUsers
{
	public static final String TEXAS_TIMEZONE					= "GMT-6";
	public static final int NUM_EVENT_THRESHOLD					= 10;
	public static final double INSERT_LOSS_TOLERANCE			= 0.0;
	public static final double UPD_LOSS_TOLERANCE				= 0.5;
	
	public static final String COUNTY_INFO_FILE 	
																= "/proj/MobilityFirst/ayadavDir/contextServiceScripts/countyData.csv";
	
	//public static final String COUNTY_INFO_FILE 	
	//															= "/home/ayadav/Documents/Data/CountyPopulation/countyData.csv";
	
	//public  static final String USER_TRACE_DIR					= "/home/ayadav/Documents/Data/confidentialUserTraces/processedIndividualTracesDupRem";;

	public  static final String USER_TRACE_DIR					= "/proj/MobilityFirst/ayadavDir/contextServiceScripts/processedIndividualTracesDupRem";
	
	//public static final String USER_TRACE_DIRECTORY 
	//= 
	
	public static final String USER_INFO_FILE_PREFIX			= "UserInfo";
	
	
	// latitude longitude key in json and attribute names in CNS
	public static final String LATITUDE_KEY						= "latitude";
	public static final String LONGITUDE_KEY					= "longitude";
	
	
	public static final long  TIME_UPDATE_SLEEP_TIME			= 60*1000;  // every minute
	
	
	public static final long TIME_DIST_INTERVAL					= 60*10; // 10 minutes on either side.
	
	// 1475465005.550073
	// actual date is 2016-11-01 22:28:35 +0000
	// Update requests that have timestamp >= startUnixTime are sent to CNS
	public static final long START_UNIX_TIME					= 1478013315;
	
	//1484538143
	// actual date is Mon, 16 Jan 2017 03:42:23 GMT
	//public static final long END_UNIX_TIME					= 1484538143;
	
	public static final long END_UNIX_TIME						= 1478014215;
	
	
	//"geoLocationCurrentTimestamp"
	public static final String GEO_LOC_TIME_KEY					= "geoLocationCurrentTimestamp";
	
	
	// "geoLocationCurrent"
	public static final String GEO_LOC_KEY						= "geoLocationCurrent";
	
	//"coordinates"
	public static final String COORD_KEY						= "coordinates";
	
	public static final String GEOLOC_TIME						= "geoLocationCurrentTimestamp";
	
	
	private static String csHost 								= "";
	private static int csPort 									= -1;
	
	public static List<CountyNode> countyProbList;
	
	public static long numusers;
	
	public static int myID										= 0;
	
	public static double initRate								= 100.0;
	
	public static String guidPrefix 							= "GUID_PREFIX";
	
	public static long currRealUnixTime							= START_UNIX_TIME;
	
	// two users files are written. When UserInfo1 is read then UserInfo2 is written
	// and when 2 is read to perform updates then UserInfo1 is written for next updates.
	public static int userinfoFileNum							= 0;
	
	
//	public static String[] filenameArray 						= {"TraceUser144.txt", "TraceUser182.txt", "TraceUser214.txt", "TraceUser218.txt"
//																		, "TraceUser173.txt", "TraceUser194.txt", "TraceUser187.txt", "TraceUser190.txt"
//																		, "TraceUser154.txt", "TraceUser181.txt"};
	public static List<String> filenameList;
	public static ContextServiceClient csClient;
	
	public static Random distibutionRand;
	
	public static boolean performInit							= false;;
	
	
	public static long computeSumPopulation()
	{
		BufferedReader readfile = null;
		long totalPop = 0;
		
		try
		{
			String sCurrentLine;
			readfile = new BufferedReader(new FileReader(COUNTY_INFO_FILE));
			
			while( (sCurrentLine = readfile.readLine()) != null )
			{
				String[] parsed = sCurrentLine.split(",");
				
				if( parsed.length >= 8)
					totalPop = totalPop + Long.parseLong(parsed[7]);
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
				if (readfile != null)
					readfile.close();				
			} 
			catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
		return totalPop;
	}
	
	
	private static void computeCountyPopDistribution(long totalPop)
	{
		BufferedReader readfile = null;
		
		try
		{
			String sCurrentLine;
			readfile = new BufferedReader(new FileReader(COUNTY_INFO_FILE));
			
			double lastUpperBound = 0.0;
			
			while( (sCurrentLine = readfile.readLine()) != null )
			{
				String[] parsed = sCurrentLine.split(",");
				
				if( !(parsed.length >= 8) )
					continue;
					
				
				int statefp = Integer.parseInt(parsed[0]);
				int countyfp = Integer.parseInt(parsed[1]);
				String countyname = parsed[2];
				double minLat = Double.parseDouble(parsed[3]);
				double minLong = Double.parseDouble(parsed[4]);
				double maxLat = Double.parseDouble(parsed[5]);
				double maxLong = Double.parseDouble(parsed[6]);
				long countypop = Long.parseLong(parsed[7]);
				
				double prob = (countypop*1.0)/(totalPop*1.0);
				
				CountyNode countynode = new CountyNode();
				countynode.statefp = statefp;
				countynode.countyfp = countyfp;
				countynode.countyname = countyname;
				countynode.minLat = minLat;
				countynode.minLong = minLong;
				countynode.maxLat = maxLat;
				countynode.maxLong = maxLong;
				countynode.population = countypop;
				
				
				countynode.lowerProbBound = lastUpperBound;
				countynode.upperProbBound = countynode.lowerProbBound + prob;
				
				lastUpperBound = countynode.upperProbBound;
				
				countyProbList.add(countynode);
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
				if (readfile != null)
					readfile.close();				
			} catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
	}
	
	/**
	 * Implements the binary search of county in which the given random
	 * variable lies. The code implements choosing a county to add a person based
	 * on counties population. Binary search is needed because we have 3000 counties
	 * Linear search each time will be expensive.
	 * @return
	 */
	public static CountyNode binarySearchOfCounty(double randomVal)
	{
		int lowerBound = 0;
		int upperBound = countyProbList.size() -1;
		int mid = (lowerBound+upperBound)/2;
		
		boolean cont = true;
		CountyNode retNode = null;
		do
		{
			CountyNode countynode = countyProbList.get(mid);
			if( (randomVal >= countynode.lowerProbBound) && 
						(randomVal <countynode.upperProbBound) )
			{
				retNode = countynode;
				break;
			}
			else
			{
				if( randomVal < countynode.lowerProbBound )
				{
					upperBound = mid-1;
					assert(upperBound >=0);
					mid = (lowerBound+upperBound)/2;
				}
				else if( randomVal >= countynode.upperProbBound )
				{
					lowerBound = mid+1;
					assert(lowerBound < countyProbList.size());
					mid = (lowerBound+upperBound)/2;
				}
				else
				{
					assert(false);
				}
			}
		} 
		while(cont);
		assert(retNode != null);
		return retNode;
	}
	
	
	public static boolean checkIfRelativeTimeInTimeSlot(long relativeTimeFromMidnight) 
																				throws ParseException
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
	
	
	private static void computeGlobalLatLongBounds()
	{
		double minLat    = 1000;
		double minLong   = 1000;
		double maxLat    = -1000;
		double maxLong   = -1000;
		
		for(int i=0; i<countyProbList.size(); i++)
		{
			CountyNode countynode = countyProbList.get(i);
			
			if(  countynode.minLat < minLat )
			{
				minLat = countynode.minLat;
			}
			
			
			if( countynode.minLong < minLong )
			{
				minLong = countynode.minLong;
			}
			
			if( countynode.maxLat > maxLat )
			{
				maxLat = countynode.maxLat;
			}
			
			
			if( countynode.maxLong > maxLong )
			{
				maxLong = countynode.maxLong;
			}
		}	
		System.out.println("minLat="+minLat+", minLong="+minLong
					+", maxLat="+maxLat+", maxLong="+maxLong);
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
					sdf.setTimeZone(TimeZone.getTimeZone(TEXAS_TIMEZONE)); 
					// give a timezone reference for formating (see comment at the bottom
					String dateFormat = sdf.format(date);
					
					String printStr = "Current date "+dateFormat;
					
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
		numusers 	= Long.parseLong(args[0]);
		csHost 		= args[1];
		csPort 		= Integer.parseInt(args[2]);
		initRate 	= Double.parseDouble(args[3]);
		myID        = Integer.parseInt(args[4]);
		performInit = Boolean.parseBoolean(args[5]);
		
		
		distibutionRand = new Random((myID+1)*100);
		
		// compute distributions
		DistributionLearningFromTraces.main(null);
		
		csClient  	= new ContextServiceClient(csHost, csPort, false, 
				PrivacySchemes.NO_PRIVACY);
		
		
		guidPrefix 	= guidPrefix+myID;
		
		//userInfoMap = new HashMap<String, UserRecordInfo>();
		
		countyProbList = new LinkedList<CountyNode>();
		
		filenameList   = new LinkedList<String>();
		
		Iterator<String> filenameIter 
					   = DistributionLearningFromTraces.distributionsMap.keySet().iterator();
		
		while( filenameIter.hasNext() )
		{
			String filename = filenameIter.next();
			filenameList.add(filename);
		}
		
		
		long totalPop  = computeSumPopulation();
		
		computeCountyPopDistribution(totalPop);
		
		computeGlobalLatLongBounds();
		
		
		if(performInit)
		{
			UserInitializationClass userInitObj = new UserInitializationClass();
			userInitObj.initializaRateControlledRequestSender();
		}
		else
		{
			UserInfoFileWriting userInfoFileWrit = new UserInfoFileWriting();
			userInfoFileWrit.initializaRateControlledRequestSender();
		}
		
		
		
		TimerThread timerThread = new TimerThread();
		new Thread(timerThread).start();
		
		try
		{
			TraceBasedUpdate traceBasedUpdate = new TraceBasedUpdate();
			traceBasedUpdate.rateControlledRequestSender();
		}
		catch(Exception | Error ex)
		{
			ex.printStackTrace();
		}
		
		System.exit(0);
	}
}