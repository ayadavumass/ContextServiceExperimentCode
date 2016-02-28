package edu.umass.cs.genericExpClient;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;

public class SearchAndUpdateDriver
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
	
	public static final double ATTR_MIN 						= 1.0;
	public static final double ATTR_MAX 						= 1500.0;
	
	public static final String attrPrefix						= "attr";
	
	public static double numUsers 								= -1;
	
	
	//2% of domain queried
	//public static final double percDomainQueried				= 0.35;
	
	private static String gnsHost 								= "";
	private static int gnsPort 									= -1;
	
	private static String csHost 								= "";
	private static int csPort 									= -1;
	
	public static String guidPrefix								= "UserGUID";
	
	// it set to true then GNS is used for updates
	// otherwise updates are directly sent to context service.
	public static final boolean useGNS							= false;
	
	
	public static UniversalTcpClient gnsClient;
	public static GuidEntry accountGuid;
	
	//public static HashMap<String, UserRecordInfo> userInfoHashMap;
	
	public static ExecutorService taskES;
	
	public static int myID;
	
	public static boolean useContextService						= false;
	private static boolean updateEnable							= false;
	private static boolean searchEnable							= false;
	
	public static ContextServiceClient<String> csClient;
		
	// per sec
	public static double initRate								= 1.0;
	public static double searchQueryRate						= 1.0; //about every 300 sec
	public static double updateRate								= 1.0; //about every 300 sec
	
	public static int numAttrs									= 1;
	
	public static int numAttrsInQuery							= 1;
	
	public static double rhoValue								= 0.5;
	
	
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
		myID = Integer.parseInt(args[8]);
		initRate = Double.parseDouble(args[9]);
		searchQueryRate = Double.parseDouble(args[10]);
		updateRate = Double.parseDouble(args[11]);
		numAttrs = Integer.parseInt(args[12]);
		numAttrsInQuery = Integer.parseInt(args[13]);
		rhoValue = Double.parseDouble(args[14]);
		
		System.out.println("Search and update client started ");
		guidPrefix = guidPrefix+myID;
		
		gnsClient = new UniversalTcpClient(gnsHost, gnsPort, true);
		csClient = new ContextServiceClient<String>(csHost, csPort);
		System.out.println("ContextServiceClient created");
		// per 1 ms
		//locationReqsPs = numUsers/granularityOfGeolocationUpdate;
		//userInfoHashMap = new HashMap<String, UserRecordInfo>();
		//taskES = Executors.newCachedThreadPool();
		
		taskES = Executors.newFixedThreadPool(2000);
		long start = System.currentTimeMillis();
		new UserInitializationClass().initializaRateControlledRequestSender();
		long end = System.currentTimeMillis();
		System.out.println(numUsers+" initialization complete "+(end-start));
		
		//LocationUpdateFixedUsers locUpdate = null;
		UpdateFixedUsers locUpdate = null;
		UniformQueryClass searchQClass = null;
		BothSearchAndUpdate bothSearchAndUpdate = null;
		
		if(updateEnable && !searchEnable)
		{
			locUpdate = new UpdateFixedUsers();
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
		
		System.exit(0);
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
	
//	public static JSONObject getUpdateJSONForCS(int userState, double userLat, 
//	double userLong) throws JSONException
//{
//JSONObject attrValJSON = new JSONObject();
//attrValJSON.put(latitudeAttrName, userLat);
//attrValJSON.put(longitudeAttrName, userLong);
//attrValJSON.put(userStateAttrName, userState);
//return attrValJSON;
//}

//public static JSONObject getUpdateJSONForGNS(int userState, double userLat, 
//	double userLong) throws JSONException
//{
//JSONObject attrValJSON = new JSONObject();
//attrValJSON.put(GEO_LOCATION_CURRENT, 
//		GeoJSON.createGeoJSONCoordinate(new GlobalCoordinate(userLat, userLong)));
//attrValJSON.put(userStateAttrName, userState);
//return attrValJSON;
//}
}