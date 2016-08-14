package edu.umass.cs.genericExpClientCallBack;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.RefreshTrigger;
import edu.umass.cs.gnsclient.client.GuidEntry;

public class SearchAndUpdateDriver
{
	// 100 seconds, experiment runs for 100 seconds
	public static 	 int EXPERIMENT_TIME						= 100000;
	
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
	
	// every 1000 msec, 0 is immidiate reading
	public static final int TRIGGER_READING_INTERVAL			= 0;
	
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
	
	//public static UniversalTcpClient gnsClient;
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
	
	public static boolean triggerEnable							= false;
	
	public static boolean searchUpdateSeparate					= false;
	
	public static boolean userInitEnable						= true;
	
	public static boolean singleRequest							= false;
	
	public static int transformType								= -1;
	
	public static double predicateLength						= 0.5;
	
	
	public static void main( String[] args ) throws Exception
	{
		ContextServiceLogger.getLogger().setLevel(Level.INFO);
		
		if( args.length >= 18 )
		{
			numUsers 		  = Double.parseDouble(args[0]);
			gnsHost  		  = args[1];
			gnsPort  		  = Integer.parseInt(args[2]);
			csHost   		  = args[3];
			csPort   		  = Integer.parseInt(args[4]);
			useContextService = Boolean.parseBoolean(args[5]);
			updateEnable 	  = Boolean.parseBoolean(args[6]);
			searchEnable 	  = Boolean.parseBoolean(args[7]);
			myID 			  = Integer.parseInt(args[8]);
			initRate 		  = Double.parseDouble(args[9]);
			searchQueryRate   = Double.parseDouble(args[10]);
			updateRate 		  = Double.parseDouble(args[11]);
			numAttrs 		  = Integer.parseInt(args[12]);
			numAttrsInQuery   = Integer.parseInt(args[13]);
			rhoValue 		  = Double.parseDouble(args[14]);
			triggerEnable	  = Boolean.parseBoolean(args[15]);
			searchUpdateSeparate = Boolean.parseBoolean(args[16]);
			userInitEnable	  = Boolean.parseBoolean(args[17]);
			singleRequest     = Boolean.parseBoolean(args[18]);
			//transformType     = Integer.parseInt(args[19]);
			transformType     = ContextServiceClient.HYPERSPACE_BASED_CS_TRANSFORM;
			predicateLength   = Double.parseDouble(args[19]);
		}
		else
		{
			// for local test
			numUsers 		  = 100.0;
			gnsHost  		  = "127.0.0.1";
			gnsPort  		  = 24398;
			csHost   		  = "127.0.0.1";
			csPort   		  = 8000;
			useContextService = true;
			updateEnable 	  = true;
			searchEnable 	  = true;
			myID 			  = 0;
			initRate 		  = 200.0;
			searchQueryRate   = 0.1;
			updateRate 		  = 0.1;
			numAttrs 		  = 6;
			numAttrsInQuery   = 2;
			rhoValue 		  = 0.0;
			triggerEnable	  = false;
			searchUpdateSeparate = false;
			userInitEnable	  = true;
			transformType     = ContextServiceClient.HYPERSPACE_BASED_CS_TRANSFORM;
		}
		
		System.out.println("Search and update client started ");
		guidPrefix = guidPrefix+myID;
		
		//gnsClient = new UniversalTcpClient(gnsHost, gnsPort, true);
		csClient  = new ContextServiceClient<String>(csHost, csPort, transformType);
		
		System.out.println("ContextServiceClient created");
		// per 1 ms
		//locationReqsPs = numUsers/granularityOfGeolocationUpdate;
		//userInfoHashMap = new HashMap<String, UserRecordInfo>();
		//taskES = Executors.newCachedThreadPool();
		if( triggerEnable )
		{
			new Thread( new ReadTriggerRecvd() ).start();
		}
		
		taskES = Executors.newFixedThreadPool(1);
		
		if( userInitEnable )
		{
			long start 	= System.currentTimeMillis();
			new UserInitializationClass().initializaRateControlledRequestSender();
			long end 	= System.currentTimeMillis();
			System.out.println(numUsers+" initialization complete "+(end-start));
		}
		
		// LocationUpdateFixedUsers locUpdate = null;
		UpdateFixedUsers locUpdate 			 	= null;
		UniformQueryClass searchQClass 		 	= null;
		BothSearchAndUpdate bothSearchAndUpdate = null;
		
		
		if( !searchUpdateSeparate )
		{
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
				double avgUpdateLatency = bothSearchAndUpdate.getAverageUpdateLatency();
				double avgSearchLatency = bothSearchAndUpdate.getAverageSearchLatency();
				long numUpdates = bothSearchAndUpdate.getNumUpdatesRecvd();
				long numSearches = bothSearchAndUpdate.getNumSearchesRecvd();
				System.out.println("avgUpdateLatency "+avgUpdateLatency
						+" avgSearchLatency "+avgSearchLatency
						+" numUpdates "+numUpdates
						+" numSearches "+numSearches);
				csClient.printTriggerStats();
			}
		}
		else
		{
			// just running so that system has queries average number of queries before
			SearchAndUpdateDriver.EXPERIMENT_TIME = 30000;
			searchQClass = new UniformQueryClass();
			searchQClass.run();
			
			SearchAndUpdateDriver.EXPERIMENT_TIME = 100000;
//			locUpdate = new UpdateFixedUsers();
//			new Thread(locUpdate).start();
			bothSearchAndUpdate = new BothSearchAndUpdate();
			new Thread(bothSearchAndUpdate).start();
			
			bothSearchAndUpdate.waitForThreadFinish();
		}
		System.exit(0);
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
	
	public static class ReadTriggerRecvd implements Runnable
	{
		@Override
		public void run()
		{
			while(true)
			{
				JSONArray triggerArray = new JSONArray();
				csClient.getQueryUpdateTriggers(triggerArray);
				
//				System.out.println("Reading triggers num read "
//												+triggerArray.length());
				
				for( int i=0;i<triggerArray.length();i++ )
				{
					try 
					{
						RefreshTrigger<Integer> refreshTrig 
							= new RefreshTrigger<Integer>(triggerArray.getJSONObject(i));
						long timeTakenSinceUpdate 
							= System.currentTimeMillis() - refreshTrig.getUpdateStartTime();
						if(timeTakenSinceUpdate <= 0)
						{
							System.out.println("Trigger recvd time sync issue between two machines ");
						}
						else
						{
							System.out.println("Trigger recvd time taken "+timeTakenSinceUpdate);
						}
					} catch (JSONException e) 
					{
						e.printStackTrace();
					}
				}
				
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