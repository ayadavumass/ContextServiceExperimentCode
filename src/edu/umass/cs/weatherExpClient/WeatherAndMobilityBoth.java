package edu.umass.cs.weatherExpClient;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.acs.geodesy.GlobalCoordinate;
import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;

public class WeatherAndMobilityBoth 
{
	// 100 seconds, experiment runs for 100 seconds
	public static final int EXPERIMENT_TIME						= 100000;
	
	// 1% loss tolerance
	public static final double INSERT_LOSS_TOLERANCE			= 0.5;
		
	// 1% loss tolerance
	public static final double UPD_LOSS_TOLERANCE				= 0.5;
	
	// 1% loss tolerance
	public static final double SEARCH_LOSS_TOLERANCE			= 0.5;
	
	// after sending all the requests it waits for 100 seconds 
	public static final int WAIT_TIME							= 100000;
	
	public static final double LONGITUDE_MIN 					= -98.08;
	public static final double LONGITUDE_MAX 					= -96.01;
	
	public static final double LATITUDE_MAX 					= 33.635;
	public static final double LATITUDE_MIN 					= 31.854;
	
	// every 1000 msec 
	public static final int TRIGGER_READING_INTERVAL			= 1000;
	
	public static double numUsers 								= -1;
	
	//2% of domain queried
	//public static final double percDomainQueried				= 0.35;
	
	private static String gnsHost 								= "";
	private static int gnsPort 									= -1;
	
	private static String csHost 								= "";
	private static int csPort 									= -1;
	
	public static String guidPrefix								= "UserGUID";
	
	// state of a user
	public static final int STATE_DRIVING						= 1;
	public static final int STATE_WALKING						= 2;
	public static final int STATE_STATIONARY					= 3;
	
	
	public static final int SPEED_DRIVING						= 40;  // 40 mph
	public static final int SPEED_WALKING						= 3;   // 3 mph
	private static final int SPEED_STATIONARY					= 0;
	
	//GNS fields
	public static final String GEO_LOCATION_CURRENT 			= "geoLocationCurrent"; // Users current location (dynamic)
	
	//CS fields
	public static final String latitudeAttrName					= "geoLocationCurrentLat";
	public static final String longitudeAttrName				= "geoLocationCurrentLong";
	
	//common to both
	public static final String userStateAttrName				= "userState";
	
	public static double granularityOfInitialization			= 300000; //about every 300 sec
	public static double granularityOfGeolocationUpdate			= 10000;  //about every 10 sec
	
	public static double granularityOfStateChange				= 1*60*1000; //about every  1 min, will be changed to 15 min
	
	// it set to true then GNS is used for updates
	// otherwise updates are directly sent to context service.
	public static final boolean useGNS							= false;
	
	
	public static GNSClientCommands gnsClient;
	public static GuidEntry accountGuid;
	
	public static HashMap<String, UserRecordInfo> userInfoHashMap;
	
	public static ExecutorService taskES;
	
	public static int myID;
	
	public static boolean useContextService					= false;
	private static boolean updateEnable						= false;
	private static boolean searchEnable						= false;
	
	public static ContextServiceClient<String> csClient;
	
	
	
	private static final double LEFT = LONGITUDE_MIN;
	private static final double RIGHT = LONGITUDE_MAX;
	private static final double TOP = LATITUDE_MAX;
	private static final double BOTTOM = LATITUDE_MIN;
	
	
	private static final GlobalCoordinate UPPER_LEFT = new GlobalCoordinate(TOP, LEFT);
	
	private static final GlobalCoordinate UPPER_RIGHT = new GlobalCoordinate(TOP, RIGHT);
	
	private static final GlobalCoordinate LOWER_RIGHT = new GlobalCoordinate(BOTTOM, RIGHT);
	
	private static final GlobalCoordinate LOWER_LEFT = new GlobalCoordinate(BOTTOM, LEFT);
	
	public static final List<GlobalCoordinate> AREA_EXTENT = new ArrayList(
	          Arrays.asList(UPPER_LEFT, UPPER_RIGHT, LOWER_RIGHT, LOWER_LEFT, UPPER_LEFT));
	
	public static final String weatherDirName 
	//= "/home/adipc/Documents/MobilityFirstGitHub/Alert-Control-System/data/ALL-12-26";
	  = "/proj/MobilityFirst/ayadavDir/ACSGithub/Alert-Control-System/data/ALL-12-26";
		
	// per sec
	public static double searchQueryRate						= 1.0; //about every 300 sec
	public static double updateRate								= 1.0; //about every 300 sec
	
	public static boolean triggerEnable							= false;
	public static boolean separateEnable						= false;
	
	
	public static void main(String[] args) throws Exception
	{
		ContextServiceLogger.getLogger().setLevel(Level.INFO);
		
		numUsers = Double.parseDouble(args[0]);
		gnsHost  = args[1];
		gnsPort  = Integer.parseInt(args[2]);
		csHost   = args[3];
		csPort   = Integer.parseInt(args[4]);
		useContextService = Boolean.parseBoolean(args[5]);
		updateEnable = Boolean.parseBoolean(args[6]);
		searchEnable = Boolean.parseBoolean(args[7]);
		granularityOfInitialization = Double.parseDouble(args[8]);
		granularityOfGeolocationUpdate = Double.parseDouble(args[9]);
		granularityOfStateChange = Double.parseDouble(args[10]);
		myID = Integer.parseInt(args[11]);
		searchQueryRate = Double.parseDouble(args[12]);
		updateRate = Double.parseDouble(args[13]);
		triggerEnable = Boolean.parseBoolean(args[14]);
		separateEnable = Boolean.parseBoolean(args[15]);
		
		guidPrefix = guidPrefix+myID;
		
		gnsClient = new GNSClientCommands();
		csClient = new ContextServiceClient<String>(csHost, csPort, 
				ContextServiceClient.SUBSPACE_BASED_CS_TRANSFORM);
	
		// per 1 ms
		//locationReqsPs = numUsers/granularityOfGeolocationUpdate;
		
		userInfoHashMap = new HashMap<String, UserRecordInfo>();
	
		if( triggerEnable )
		{
			new Thread(new ReadTriggerRecvd()).start();
		}
		
		//taskES = Executors.newCachedThreadPool();
		taskES = Executors.newFixedThreadPool(2000);
		long start = System.currentTimeMillis();
		new UserInitializationClass().initializaRateControlledRequestSender();
		long end = System.currentTimeMillis();
		System.out.println(numUsers+" initialization complete "+(end-start));
		
		//LocationUpdateFixedUsers locUpdate = null;
		LocationUpdateFixedMovingUsers locUpdate = null;
		UniformQueryClass searchQClass = null;
		BothSearchAndUpdate bothSearchAndUpdate = null;
		
		if( !separateEnable )
		{
			if(updateEnable && !searchEnable)
			{
				locUpdate = new LocationUpdateFixedMovingUsers();
				new Thread(locUpdate).start();
			}
			else if(searchEnable && !updateEnable)
			{
				searchQClass = new UniformQueryClass();
				new Thread(searchQClass).start();
			}
			else if(searchEnable && updateEnable)
			{
				bothSearchAndUpdate = new BothSearchAndUpdate();
				new Thread(bothSearchAndUpdate).start();
			}
			
			if(updateEnable && !searchEnable)
			{
				locUpdate.waitForThreadFinish();
			}
			else if(searchEnable && !updateEnable)
			{
				searchQClass.waitForThreadFinish();
			}
			else if(searchEnable && updateEnable)
			{
				bothSearchAndUpdate.waitForThreadFinish();
			}
		}
		else
		{
			searchQClass = new UniformQueryClass();
			searchQClass.run();
			
			locUpdate = new LocationUpdateFixedMovingUsers();
			new Thread(locUpdate).start();
			locUpdate.waitForThreadFinish();
		}
		
		// based on weather and mobility model
//		LocationUpdateClass locUpdate = null;
//		WeatherQueryClass searchQClass = null;
//		if(updateEnable)
//		{
//			locUpdate = new LocationUpdateClass();
//			new Thread(locUpdate).start();
//		}
//		if(searchEnable)
//		{
//			searchQClass = new WeatherQueryClass();
//			new Thread(searchQClass).start();
//		}
//		
//		
//		if(updateEnable)
//		{
//			locUpdate.waitForThreadFinish();
//		}
//		if(searchEnable)
//		{
//			searchQClass.waitForThreadFinish();
//		}
		
		System.exit(0);
	}
	
	public static JSONObject getUpdateJSONForCS(int userState, double userLat, 
			double userLong) throws JSONException
	{
		JSONObject attrValJSON = new JSONObject();
		attrValJSON.put(latitudeAttrName, userLat);
		attrValJSON.put(longitudeAttrName, userLong);
		attrValJSON.put(userStateAttrName, userState);
		return attrValJSON;
	}
	
	public static JSONObject getUpdateJSONForGNS(int userState, double userLat, 
			double userLong) throws JSONException
	{
		JSONObject attrValJSON = new JSONObject();
		attrValJSON.put(GEO_LOCATION_CURRENT, 
				GeoJSON.createGeoJSONCoordinate(new GlobalCoordinate(userLat, userLong)));
		attrValJSON.put(userStateAttrName, userState);
		return attrValJSON;
	}
	
	public static String getSHA1(String stringToHash)
	{
	   MessageDigest md=null;
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
	
	
	public static class ReadTriggerRecvd implements Runnable
	{
		@Override
		public void run()
		{
			while(true)
			{
				JSONArray triggerArray = new JSONArray();
				csClient.getQueryUpdateTriggers(triggerArray);
				
				System.out.println("Reading triggers num read "
												+triggerArray.length());
				
				try
				{
					Thread.sleep(TRIGGER_READING_INTERVAL);
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
	}
}