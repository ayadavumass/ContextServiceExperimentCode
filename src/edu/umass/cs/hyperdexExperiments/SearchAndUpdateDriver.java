package edu.umass.cs.hyperdexExperiments;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hyperdex.client.Client;

import edu.umass.cs.gnsclient.client.util.GuidEntry;

public class SearchAndUpdateDriver
{
	public static final String COORD_IP							= "compute-0-23";
	public static final int COORD_PORT							= 4999;
	public static final String HYPERSPACE_NAME					= "contextnet";
	public static final String GUID_ATTR_NAME					= "GUID";
	
	// 100 seconds, experiment runs for 100 seconds
	public static 	 long EXPERIMENT_TIME						= 100000;
	
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
	
//	private static String gnsHost 								= "";
//	private static int gnsPort 									= -1;
//	private static String csHost 								= "";
//	private static int csPort 									= -1;
	
	public static String guidPrefix								= "UserGUID";
	
	// it set to true then GNS is used for updates
	// otherwise updates are directly sent to context service.
//	public static final boolean useGNS							= false;
	
	//public static UniversalTcpClient gnsClient;
	public static GuidEntry accountGuid;
	
	//public static HashMap<String, UserRecordInfo> userInfoHashMap;
	public static ExecutorService taskES;
	
	public static int myID;
	
	//public static boolean useContextService						= false;
	private static boolean updateEnable							= false;
	private static boolean searchEnable							= false;
	
//	public static ContextServiceClient<String> csClient;
	
	public static Queue<Client> hyperdexClients;
	
	// per sec
	public static double initRate								= 1.0;
	public static double searchQueryRate						= 1.0; //about every 300 sec
	public static double updateRate								= 1.0; //about every 300 sec
	
	public static int numAttrs									= 1;
	
	public static int numAttrsInQuery							= 1;
	
	public static double rhoValue								= 0.5;
	
//	public static boolean triggerEnable							= false;
	
	public static boolean searchUpdateSeparate					= false;
	
	public static boolean userInitEnable						= true;
	
	public static boolean singleRequest							= false;
	
//	public static int transformType								= -1;
	
	public static double predicateLength						= 0.5;
	
	// in msec
//	public static long queryExpiryTime							= 30000;
	public static int numberHClients							= 10;
	
	public static final Object clientLock						= new Object();
	
	public static void main( String[] args ) throws Exception
	{	
		numUsers 		  = Double.parseDouble(args[0]);
		updateEnable 	  = Boolean.parseBoolean(args[1]);
		searchEnable 	  = Boolean.parseBoolean(args[2]);
		myID 			  = Integer.parseInt(args[3]);
		initRate 		  = Double.parseDouble(args[4]);
		searchQueryRate   = Double.parseDouble(args[5]);
		updateRate 		  = Double.parseDouble(args[6]);
		numAttrs 		  = Integer.parseInt(args[7]);
		numAttrsInQuery   = Integer.parseInt(args[8]);
		rhoValue 		  = Double.parseDouble(args[9]);
		userInitEnable	  = Boolean.parseBoolean(args[10]);
		singleRequest     = Boolean.parseBoolean(args[11]);
		predicateLength   = Double.parseDouble(args[12]);
		EXPERIMENT_TIME   = Long.parseLong(args[13]);
		numberHClients    = Integer.parseInt(args[14]);
		
		System.out.println("Search and update client started ");
		guidPrefix = guidPrefix+myID;
		
		hyperdexClients = new LinkedList<Client>();
		
		for( int i=0; i<numberHClients; i++ )
		{
			Client hyperdexClient = new Client(COORD_IP, COORD_PORT);
			hyperdexClients.add(hyperdexClient);
		}
		
		//gnsClient = new UniversalTcpClient(gnsHost, gnsPort, true);
		//csClient  = new ContextServiceClient<String>(csHost, csPort, transformType);
		
		System.out.println("Hyperdex clients created");
		// per 1 ms
		//locationReqsPs = numUsers/granularityOfGeolocationUpdate;
		//userInfoHashMap = new HashMap<String, UserRecordInfo>();
		//taskES = Executors.newCachedThreadPool();
//		if( triggerEnable )
//		{
//			new Thread( new ReadTriggerRecvd() ).start();
//		}
		
		taskES = Executors.newFixedThreadPool(numberHClients);
		
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
				//locUpdate = new UpdateFixedUsers();
				//new Thread(locUpdate).start();
			}
			else if(searchEnable && !updateEnable)
			{
				//searchQClass = new UniformQueryClass();
				//new Thread(searchQClass).start();
			}
			else if(searchEnable && updateEnable)
			{
				bothSearchAndUpdate = new BothSearchAndUpdate();
				new Thread(bothSearchAndUpdate).start();
			}
			
			if(updateEnable && !searchEnable)
			{
				//locUpdate.waitForThreadFinish();
			}
			else if(searchEnable && !updateEnable)
			{
				//searchQClass.waitForThreadFinish();
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
				//csClient.printTriggerStats();
			}
		}
		else
		{
			// just running so that system has queries average number of queries before
			SearchAndUpdateDriver.EXPERIMENT_TIME = 60000;
			searchQClass = new UniformQueryClass();
			searchQClass.run();
			
			SearchAndUpdateDriver.EXPERIMENT_TIME = 100000;
//			locUpdate = new UpdateFixedUsers();
//			new Thread(locUpdate).start();
			bothSearchAndUpdate = new BothSearchAndUpdate();
			new Thread(bothSearchAndUpdate).start();
			
			bothSearchAndUpdate.waitForThreadFinish();
			
			double avgUpdateLatency = bothSearchAndUpdate.getAverageUpdateLatency();
			double avgSearchLatency = bothSearchAndUpdate.getAverageSearchLatency();
			long numUpdates = bothSearchAndUpdate.getNumUpdatesRecvd();
			long numSearches = bothSearchAndUpdate.getNumSearchesRecvd();
			System.out.println("avgUpdateLatency "+avgUpdateLatency
					+" avgSearchLatency "+avgSearchLatency
					+" numUpdates "+numUpdates
					+" numSearches "+numSearches);
			//csClient.printTriggerStats();
			
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
	
	
	public static Client getHyperdexClient()
	{
		synchronized(clientLock)
		{
			while( hyperdexClients.size() == 0 )
			{
				try 
				{
					clientLock.wait();
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
			Client hClient = hyperdexClients.poll();
			return hClient;
		}
	}
	
	public static void returnHyperdexClient(Client hClient)
	{
		synchronized(clientLock)
		{
			hyperdexClients.add(hClient);
			clientLock.notify();
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