package edu.umass.cs.weatherExpClient;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.acs.geodesy.GlobalCoordinate;
import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;

public class WeatherModelOld 
{
	private static final double LEFT = -98.08;
	private static final double RIGHT = -96.01;
	private static final double TOP = 33.635;
	private static final double BOTTOM = 31.854;
	  

	private static final GlobalCoordinate UPPER_LEFT = new GlobalCoordinate(TOP, LEFT);
	
	private static final GlobalCoordinate UPPER_RIGHT = new GlobalCoordinate(TOP, RIGHT);
	
	private static final GlobalCoordinate LOWER_RIGHT = new GlobalCoordinate(BOTTOM, RIGHT);
	
	private static final GlobalCoordinate LOWER_LEFT = new GlobalCoordinate(BOTTOM, LEFT);

	public static final List<GlobalCoordinate> AREA_EXTENT = new ArrayList(
	          Arrays.asList(UPPER_LEFT, UPPER_RIGHT, LOWER_RIGHT, LOWER_LEFT, UPPER_LEFT));
	
	public static final String weatherDirName 
	//= "/home/adipc/Documents/MobilityFirstGitHub/Alert-Control-System/data/ALL-12-26";
	  = "/proj/MobilityFirst/ayadavDir/ACSGithub/Alert-Control-System/data/ALL-12-26";
	
	// 100 seconds, experiment runs for 100 seconds
	public static final int EXPERIMENT_TIME						= 100000;
		
	// 1% loss tolerance
	public static final double SEARCH_LOSS_TOLERANCE			= 0.5;
			
	// after sending all the requests it waits for 100 seconds 
	public static final int WAIT_TIME							= 100000;
		
	private static String gnsHost 								= "";
	private static int gnsPort 									= -1;
		
	private static String csHost 								= "";
	private static int csPort 									= -1;
		
	//GNS fields
	public static final String GEO_LOCATION_CURRENT 			= "geoLocationCurrent"; // Users current location (dynamic)
		
	//CS fields
	public static final String latitudeAttrName					= "geoLocationCurrentLat";
	public static final String longitudeAttrName				= "geoLocationCurrentLong";
	
	//common to both
	public static final String userStateAttrName				= "userState";
		
	// per sec
	public static double searchQueryRate						= 1.0; //about every 300 sec
		
		
	// it set to true then GNS is used for updates
	// otherwise updates are directly sent to context service.
	public static final boolean useGNS							= false;
		
		
	public static GNSClientCommands gnsClient;
	public static GuidEntry accountGuid;
	
	public static ExecutorService taskES;
		
	public static int myID;
		
	public static boolean useContextService						= false;
		
	public static ContextServiceClient<String> csClient;
	
		
	public static void main(String[] args) throws Exception
	{
		ContextServiceLogger.getLogger().setLevel(Level.INFO);
		
		gnsHost  = args[0];
		gnsPort  = Integer.parseInt(args[1]);
		csHost   = args[2];
		csPort   = Integer.parseInt(args[3]);
		useContextService = Boolean.parseBoolean(args[4]);
		searchQueryRate = Double.parseDouble(args[5]);
		//granularityOfInitialization = Double.parseDouble(args[7]);
		//granularityOfGeolocationUpdate = Double.parseDouble(args[8]);
		//granularityOfStateChange = Double.parseDouble(args[9]);
		myID = Integer.parseInt(args[6]);
		
		gnsClient = new GNSClientCommands();
		csClient = new ContextServiceClient<String>(csHost, csPort,
				ContextServiceClient.SUBSPACE_BASED_CS_TRANSFORM);
		
		//taskES = Executors.newCachedThreadPool();
		taskES = Executors.newFixedThreadPool(1000);
		
		//JSONObject geoJSON = GeoJSON.createGeoJSONPolygon(AREA_EXTENT);
		//String overarchingQuery = 
		//"SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap(geoLocationCurrentLat, geoLocationCurrentLong, "+geoJSON.toString()+")";
		//ConcurrentHashMap<String, Boolean> replybackMap 
		//				= new ConcurrentHashMap<String, Boolean>();
		
		//csClient.sendSearchQuery(overarchingQuery, replybackMap, 300000);
		
		//System.out.println("Total number of guids in the system "
		//												+replybackMap.size());
		
		WeatherQueryClass searchQClass = new WeatherQueryClass();
		new Thread(searchQClass).start();
		
		searchQClass.waitForThreadFinish();
		
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
}