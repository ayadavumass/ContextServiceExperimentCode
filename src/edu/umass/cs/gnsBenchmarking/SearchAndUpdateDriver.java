package edu.umass.cs.gnsBenchmarking;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.packets.CommandPacket;

public class SearchAndUpdateDriver
{
	// 100 seconds, experiment runs for 100 seconds
	public static 	 long EXPERIMENT_TIME						= 100000;
	
	public static int BATCH_SIZE								= 250;
	public static final String GNS_REC_PREFIX					= "nr_valuesMap";
	
	// 1% loss tolerance
	public static final double INSERT_LOSS_TOLERANCE			= 0.0;
	
	// 1% loss tolerance
	public static final double UPD_LOSS_TOLERANCE				= 0.5;
	
	// 1% loss tolerance
	public static final double SEARCH_LOSS_TOLERANCE			= 0.5;
	
	// after sending all the requests it waits for 100 seconds 
	public static final int WAIT_TIME							= 1000000;
	
	public static final double ATTR_MIN 						= 1.0;
	public static final double ATTR_MAX 						= 1500.0;
	
	public static final String attrPrefix						= "attr";
	
	public static final int NUM_GNS_RETRIES						= 10;
	
	public static double numUsers 								= -1;
	
	//2% of domain queried
	//public static final double percDomainQueried				= 0.35;
	
	public static String guidPrefix								= "UserGUID";
	
	public static ExecutorService reqTaskES;
	
	public static ExecutorService initTaskES;
	
	public static int myID;
	
	//public static ContextServiceClient<String> csClient;
	//public static GNSClient gnsClient;
	
	// per sec
	public static double initRate								= 1.0;
	public static double requestRate							= 1.0;
	
	public static int numAttrs									= 1;
	
	public static int numAttrsInQuery							= 1;
	
	public static double rhoValue								= 0.5;
	
	public static boolean userInitEnable						= true;
	
	public static double predicateLength						= 0.5;
	
	public static int numGNSClients								= 1;
	
	public static List<GuidEntry> listOfGuidEntries				= null;
	public static final Object guidInsertLock					= new Object();
	
	public static Queue<GNSClient> gnsClientQueue				= new LinkedList<GNSClient>();
	public static final Object queueLock						= new Object();
	
	
	public static boolean useMongoDirectly						= true;
	
	public static Random rand									= new Random();
	
	public static String batchAccountAlias 						= "batchAlias";
	public static GuidEntry batchAccountGuid					= null;
	//public static String dbName;
	
	
	public static void main( String[] args ) throws Exception
	{
//		numUsers 		  	  = Double.parseDouble(args[0]);
//		myID 			  	  = Integer.parseInt(args[1]);
//		requestRate   	  	  = Double.parseDouble(args[2]);
//		numAttrs 		  	  = 20;
//		numAttrsInQuery   	  = 4;
//		rhoValue 		  	  = Double.parseDouble(args[3]);
//		predicateLength   	  = Double.parseDouble(args[4]);
//		useMongoDirectly  	  = Boolean.parseBoolean(args[5]);
//		
//		//queryExpiryTime     = Long.parseLong(args[20]);
//		threadPoolSize    	  = Integer.parseInt(args[6]);
//		int initPoolSize  	  = Integer.parseInt(args[7]);
//		initRate 		  	  = Integer.parseInt(args[8]);
//		//BATCH_SIZE			  = ;
//		
//		reqTaskES 			  = Executors.newFixedThreadPool(threadPoolSize);
//		initTaskES 			  = Executors.newFixedThreadPool(initPoolSize);
		
		
		numUsers 		  	  = Double.parseDouble(args[0]);
		myID 			  	  = 0;
		requestRate   	  	  = 100;
		numAttrs 		  	  = 20;
		numAttrsInQuery   	  = 4;
		rhoValue 		  	  = 0.5;
		predicateLength   	  = 0.5;
		useMongoDirectly  	  = false;
		
		//queryExpiryTime     = Long.parseLong(args[20]);
		numGNSClients    	  = Integer.parseInt(args[1]);
		initRate 		  	  = Integer.parseInt(args[2]);
		//BATCH_SIZE			  = ;
		
		reqTaskES 			  = Executors.newFixedThreadPool(numGNSClients);
		initTaskES 			  = Executors.newFixedThreadPool(numGNSClients);
		
		
		//if( useMongoDirectly )
		{
			//String dbName	  = args[5];
			//mongoClient = new MongoClient();
			//mongodb = mongoClient.getDatabase(dbName);
			//mongoCollection = mongodb.getCollection("NameRecord");
		}
		//else
		{
			System.out.println("Search and update client started ");
			guidPrefix = guidPrefix+myID;
			
			batchAccountAlias = batchAccountAlias + myID+"@gmail.com";
			
			for(int i=0; i<numGNSClients; i++)
			{
				GNSClient gnsClient = new GNSClient();
				gnsClient.setNumRetriesUponTimeout(NUM_GNS_RETRIES);
				gnsClientQueue.add(gnsClient);
			}
			System.out.println("[Client connected to GNS]\n");
			// per 1 ms
			//locationReqsPs = numUsers/granularityOfGeolocationUpdate;
			//userInfoHashMap = new HashMap<String, UserRecordInfo>();
			//taskES = Executors.newCachedThreadPool();
			
			listOfGuidEntries = new LinkedList<GuidEntry>();
			if( userInitEnable )
			{
//				long start 	= System.currentTimeMillis();
//				batchedAccountCreation();
//				long end 	= System.currentTimeMillis();
//				System.out.println("Batch creation of "+numUsers+" took "+(end-start)+" ms");
				
				// add index
//				System.out.println("Adding Index");
//				start = System.currentTimeMillis();
//				addAttributeIndex();
//				end = System.currentTimeMillis();
//				System.out.println("Adding index took "+(end-start));
				
				
				long start 	= System.currentTimeMillis();
				new UserInitializationClass().initializaRateControlledRequestSender();
				long end 	= System.currentTimeMillis();
				System.out.println(numUsers+" initialization complete "+(end-start));
			}
		}
		
		System.out.println(numUsers+" GUIDs created exiting ");
		System.exit(0);
		
		System.out.println("Starting workload");
		BothSearchAndUpdate bothSearchAndUpdate = null;
		
		bothSearchAndUpdate = new BothSearchAndUpdate();
		new Thread(bothSearchAndUpdate).start();
		
		bothSearchAndUpdate.waitForThreadFinish();
		double avgUpdateLatency = bothSearchAndUpdate.getAverageUpdateLatency();
		double avgSearchLatency = bothSearchAndUpdate.getAverageSearchLatency();
		long numUpdates = bothSearchAndUpdate.getNumUpdatesRecvd();
		long numSearches = bothSearchAndUpdate.getNumSearchesRecvd();
		double avgResultSize = bothSearchAndUpdate.getAvgResultSize();
		System.out.println("avgUpdateLatency "+avgUpdateLatency
				+" avgSearchLatency "+avgSearchLatency
				+" numUpdates "+numUpdates
				+" numSearches "+numSearches
				+" avgResultSize "+avgResultSize);
		
		System.exit(0);
	}
	
//	public static void addAttributeIndex()
//	{
//		//mongoClient = new MongoClient();
//		//mongodb = mongoClient.getDatabase(dbName);
//		//mongoCollection = mongodb.getCollection("NameRecord");
//		
////		GNSClient gnsClient = getGNSClient();
////		GuidEntry guidEntry = listOfGuidEntries.get(0);
//		for(int i=0; i<numAttrs; i++)
//		{
//			String attrName = attrPrefix+i;
//			String fieldName = GNS_REC_PREFIX+"."+attrName;
//			int indexName = 1;
//			
//			//Document index = new Document();
//			//index.put(fieldName, indexName);
//			//mongoCollection.createIndex(index);
//				
//				//gnsClient.execute(GNSCommand.fieldCreateIndex(guidEntry, fieldName, 
//				//		indexName));		
//		}
//	}
	
	private static void batchedAccountCreation() throws Exception
	{
		GNSClient gnsClient = getGNSClient();
		batchAccountGuid = GuidUtils.lookupOrCreateAccountGuid(gnsClient,
		              batchAccountAlias, "password", true);
		
		int numCreatedSoFar = 0;
		while(numCreatedSoFar < numUsers)
		{
			Set<String> aliases = new HashSet<>();
			
		    for (int i = 0; i < BATCH_SIZE; i++) 
		    {
		      aliases.add(guidPrefix + numCreatedSoFar+"@gmail.com");
		      numCreatedSoFar++;
		      if(numCreatedSoFar >= numUsers)
		    	  break;
		    }
		    
		    // gnsClient.setReadTimeout(15 * 1000); // 30 seconds
		    CommandPacket reply = gnsClient.execute
		    		(GNSCommand.batchCreateGUIDs(batchAccountGuid, aliases));
		}
		
	    
		CommandPacket reply = gnsClient.execute
	    			(GNSCommand.lookupAccountRecord(batchAccountGuid.getGuid()));
	    
	    JSONObject batchJSON = reply.getResultJSONObject();
	    int numGuidsCreated = batchJSON.getInt("guidCnt");
	    
	    System.out.println("numGuidsCreated "+numGuidsCreated);
	    
	    assert(numGuidsCreated == numUsers);
	    
	    System.out.println(numGuidsCreated+" guids created");
	    
//	      result = gnsClient.
//	    		  guidBatchCreate(batchAccountGuid, aliases);
//	      client.setReadTimeout(oldTimeout);
	}
	
	
	public static GNSClient getGNSClient()
	{
//		int index = rand.nextInt(gnsClientQueue.size());
//		return gnsClientQueue.get(index);
		synchronized(queueLock)
		{
			while(gnsClientQueue.size() == 0)
			{
				try 
				{
					queueLock.wait();
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
			return gnsClientQueue.poll();
		}
	}
	
	public static void returnGNSClient(GNSClient gnsClient)
	{
		synchronized(queueLock)
		{
			gnsClientQueue.add(gnsClient);
			queueLock.notify();
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
}