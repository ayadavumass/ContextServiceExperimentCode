package edu.umass.cs.expcode;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hyperdex.client.Client;
import org.hyperdex.client.HyperDexClientException;
import org.hyperdex.client.Iterator;
import org.hyperdex.client.Range;
import org.json.JSONArray;


public class JointRequestsHyperdexDirect
{
		//public static final String DUMMYGUID = "0B3C3AC6E25FF553BE3DC9176889E927C14CEA2A";
		public static final int MAX_QUERY_LEN = 10;
		
		// 100 seconds, experiment runs for 100 seconds
		public static final int EXPERIMENT_TIME										= 100000;
		
		// after sending all the requests it waits for 100 seconds 
		public static final int WAIT_TIME											= 200000;
		
		
		// decides whether to send query or update
		private static Random queryUpdateRand;
		
		private Random generalRand;
		
		// used for query workload.
		private Random queryRand;
		
		public static final String CLIENT_GUID_PREFIX								= "clientGUID";
		
		public static int NUMGUIDs													= 100;
		
		private static double currNumReqSent 										= 0;
		
		private static int clientID;
		
		public ExecutorService	 eservice											= null;
		
		// used in query work load, to keep track of current
		// number of attributes in the query
		private int currNumAttr;
		
		// used in update workload, to keep
		// track of next attribute to update.
		private int currentAttrNum;
		
		//private int numberRepliesRecvd											= 0;
		
		private long requestID														= 0;
		
		// used for generating predicate values in query
		private final Random queryGenRandom;
		
		
		//private JSONObject	attrValueObject										= null;
		
		private static int NUMATTRs;
		private static double requestsps;
		private static double queryUpdateRatio;
		
		public static final String CONTEXT_ATTR_PREFIX								= "context";
		public static final String REPLY_ADDR_KEY									= "ReplyAddress";
		public static String writerName;
		
		
		// hyperdex parameters
		// all hyperdex related constants
		// all hyperdex related constants
		//public static final String[] HYPERDEX_IP_ADDRESS 			= {"compute-0-23", "compute-0-13", "compute-0-14", "compute-0-15"};
		//public static final int[] HYPERDEX_PORT				 	= {4999, 4000, 4001, 4002};
		
		public static final String[] HYPERDEX_IP_ADDRESS 			= {"compute-0-23", "compute-0-13"};
		public static final int[] HYPERDEX_PORT				 		= {4999, 4000};
		
		//public static final String[] HYPERDEX_IP_ADDRESS 			= {"compute-0-23"};
		//public static final int[] HYPERDEX_PORT				 	= {4999};
		
		
		public static final String HYPERDEX_SPACE					= "contextnet";
		// guid is the key in hyperdex
		public static final String HYPERDEX_KEY_NAME				= "GUID";
		
		public static final int NUM_PARALLEL_CLIENTS				= 50;
		
		private final Client[] hyperdexClientArray					= new Client[NUM_PARALLEL_CLIENTS*HYPERDEX_IP_ADDRESS.length];
		
		private final ConcurrentLinkedQueue<Client> freeHClientQueue;
		
		private final Object hclientFreeMonitor						= new Object();
		
		
		public static void main(String [] args) throws Exception
		{
			// wait for loading to complete
			Thread.sleep(100000);
			
			ContextServiceLogger.getLogger().fine("Request load starting");
			clientID = Integer.parseInt(args[0]);
			writerName = "writer"+clientID;
			NUMATTRs = Integer.parseInt(args[1]);
			requestsps = Double.parseDouble(args[2]);
			
			// calculated by query/(query+update)
			queryUpdateRatio = Double.parseDouble(args[3]);
			
			NUMGUIDs = Integer.parseInt(args[4]);
			
			queryUpdateRand  = new Random(clientID);
			
			JointRequestsHyperdexDirect basicObj 
										= new JointRequestsHyperdexDirect();
			
			// should be less than 1000, for more more processes of this should be started
			//int waitInLoop = 1000/requestsps;
			long currTime = 0;
			long startTime = System.currentTimeMillis();
			double numberShouldBeSentPerSleep = requestsps/10.0;
			
			
			while( ( (System.currentTimeMillis() - startTime) < EXPERIMENT_TIME ) )
			{
				for(int i=0;i<numberShouldBeSentPerSleep;i++)
				{
					sendAMessage(basicObj);
					currNumReqSent++;
				}
				currTime = System.currentTimeMillis();
				
				double timeElapsed = ((currTime- startTime)*1.0)/1000.0;
				double numberShouldBeSentByNow = timeElapsed*requestsps;
				double needsToBeSentBeforeSleep = numberShouldBeSentByNow - currNumReqSent;
				if(needsToBeSentBeforeSleep > 0)
				{
					needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
				}
				
				for(int i=0;i<needsToBeSentBeforeSleep;i++)
				{
					sendAMessage(basicObj);
					currNumReqSent++;
				}
				Thread.sleep(100);
			}
			
			long endTime = System.currentTimeMillis();
			double timeInSec = ((double)(endTime - startTime))/1000.0;
			double sendingRate = (currNumReqSent * 1.0)/(timeInSec);
			ContextServiceLogger.getLogger().fine("Eventual sending rate "+sendingRate);
			
			Thread.sleep(WAIT_TIME);
			double endTimeReplyRecvd = System.currentTimeMillis();
			double sysThrput= (currNumReqSent * 1000.0)/(endTimeReplyRecvd - startTime);
			
			ContextServiceLogger.getLogger().fine("Result: Eventual sending rate "+sendingRate+" Throughput "+sysThrput);
			
			System.exit(0);
		}
		
		private static void sendAMessage(JointRequestsHyperdexDirect basicObj)
		{
			basicObj.requestID++;
			double randVal = queryUpdateRand.nextDouble();
			
			// send query
			if( randVal < queryUpdateRatio )
			{
				ContextServiceLogger.getLogger().fine("Sending query requestID "+basicObj.requestID+" time "+System.currentTimeMillis());
				//basicObj.queryHashMap.put( basicObj.requestID, System.currentTimeMillis() );
				basicObj.sendQuery(basicObj.requestID);
			}
			// send update
			else
			{
				//ContextServiceLogger.getLogger().fine("Update sent with ID "+basicObj.requestID);
				//basicObj.updateHashMap.put( basicObj.requestID, System.currentTimeMillis() );
				ContextServiceLogger.getLogger().fine("Sending update requestID "+basicObj.requestID+" time "+System.currentTimeMillis());
				basicObj.sendUpdate(basicObj.requestID);
			}	
		}
		
		public JointRequestsHyperdexDirect() throws Exception
		{
			// hyperdex initialization
			freeHClientQueue = new ConcurrentLinkedQueue<Client>();
			int count = 0;
			for(int j=0;j<HYPERDEX_IP_ADDRESS.length;j++)
			{
				for(int i=0;i<NUM_PARALLEL_CLIENTS;i++)
				{
					hyperdexClientArray[count] = new Client(HYPERDEX_IP_ADDRESS[j], HYPERDEX_PORT[j]);
					
					freeHClientQueue.add(hyperdexClientArray[count]);
					count++;
				}
			}
			
			currNumAttr = 1;
			// attrname starts from 0
			currentAttrNum = 0;
			
			//queryHashMap  = new ConcurrentHashMap<Long, Long>();
			//updateHashMap = new ConcurrentHashMap<Long, Long>();
			
			//waitTimer = new Timer();
			
			requestID = 0;
			
			queryRand = new Random(clientID);
			generalRand = new Random();
			//poissonRandom = new Random((Integer)myID);
			
			queryGenRandom = new Random(clientID);
			
			eservice = Executors.newFixedThreadPool(400);
		}
		
		/**
		 * Depending on the random outcome this function sends query
		 */
		public void sendQuery(long currID)
		{
			if( (currNumAttr > MAX_QUERY_LEN) || (currNumAttr > NUMATTRs) )
			{
				currNumAttr = 1;
			}
			//String query = getQueryOfSize(currNumAttr);
			Map<String, Object> queryHyperdexForm = getQueryOfSizeHyperdex(currNumAttr);
			
			eservice.execute(new SendingRequest(currID, SendingRequest.QUERY, 
					queryHyperdexForm, "") );
			currNumAttr = currNumAttr + 2;
		}
		
		/**
		 * this function sends update
		 */
		public void sendUpdate(long currID)
		{
			if( currentAttrNum >= NUMATTRs )
			{
				currentAttrNum = 0;
			}
			
			String attName = "contextATT"+currentAttrNum;
			double nextVal = 1+generalRand.nextInt((int)(1500-1));
			
			int guidID = generalRand.nextInt(NUMGUIDs);
			
			String memberAlias = CLIENT_GUID_PREFIX+clientID;
			String realAlias = memberAlias+guidID;
			String myGUID = getSHA1(realAlias);
			
			Map<String, Object> updateHyperdexForm = new HashMap<String, Object>();
			updateHyperdexForm.put(attName, nextVal);
			
			eservice.execute(new SendingRequest(currID, SendingRequest.UPDATE, updateHyperdexForm, myGUID) );
			
			currentAttrNum++;
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
	       		sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
	       }
	       return sb.toString();
		}
		
		public Map<String, Object> getQueryOfSizeHyperdex(int queryLength)
		{
			Map<String, Object> checks = new HashMap<String, Object>();
			
		    for(int i=0;i<queryLength;i++)
		    {
		    	int attrNum = queryRand.nextInt(NUMATTRs);
		    	double beg =(double) this.queryGenRandom.nextInt(1400);
		    	double end = (double)( beg+this.queryGenRandom.nextInt(1500 - (int)beg-3));
		    	
				checks.put( "contextATT"+attrNum, new Range(beg, end) );
		    }
		    return checks;
		}
		
		public class SendingRequest implements Runnable
		{
			public static final int UPDATE = 1;
			public static final int QUERY  = 2;
			
			private final int reqType;
			private final Map<String, Object> hyperdexMap;
			private final long currID;
			private final String GUID;
			
			public SendingRequest(long currID, int reqType, 
					Map<String, Object> hyperdexMap, String GUID)
			{
				this.reqType = reqType;
				this.hyperdexMap = hyperdexMap;
				this.currID = currID;
				this.GUID = GUID;
			}
			
			@Override
			public void run()
			{
				switch(reqType)
				{
					case QUERY:
					{
						//ContextServiceLogger.getLogger().fine("Sending query requestID "+currID+" time "+System.currentTimeMillis());
						sendQueryToHyperdex(hyperdexMap, currID);
						ContextServiceLogger.getLogger().fine("Query completion requestID "+currID+" time "+System.currentTimeMillis());
						break;
					}
					case UPDATE:
					{
						//ContextServiceLogger.getLogger().fine("Sending query requestID "+currID+" time "+System.currentTimeMillis());
						sendUpdateToHyperdex(currID, hyperdexMap, GUID);
						ContextServiceLogger.getLogger().fine("Update completion requestID "+currID+" time "+System.currentTimeMillis());
						break;
					}
				}
			}
			
			private void sendQueryToHyperdex(Map<String, Object> queryHyperdexForm, long userReqNum) 
			{
				Client HClinetFree = null;
				
				while( HClinetFree == null )
				{
					HClinetFree = freeHClientQueue.poll();
					
					if( HClinetFree == null )
					{
						synchronized(hclientFreeMonitor)
						{
							try
							{
								hclientFreeMonitor.wait();
							} catch (InterruptedException e)
							{
								e.printStackTrace();
							}
						}
					}
				}
				
				Iterator resultIterator = HClinetFree.search(HYPERDEX_SPACE, queryHyperdexForm);
				
				JSONArray queryAnswer = getGUIDsFromIterator(resultIterator);
				
				ContextServiceLogger.getLogger().fine("Answer size "+queryAnswer.length());
				synchronized(hclientFreeMonitor)
				{
					freeHClientQueue.add(HClinetFree);
					hclientFreeMonitor.notify();
				}
			}
			
			public void sendUpdateToHyperdex(long versionNum, Map<String, Object> updateHyperdexMap, String GUID)
			{
				Client HClinetFree = null;
				while( HClinetFree == null )
				{
					HClinetFree = freeHClientQueue.poll();
					
					if( HClinetFree == null )
					{
						synchronized(hclientFreeMonitor)
						{
							try
							{
								hclientFreeMonitor.wait();
							} catch (InterruptedException e)
							{
								e.printStackTrace();
							}
						}
					}
				}
				
				try
				{	
					HClinetFree.put( HYPERDEX_SPACE, GUID, updateHyperdexMap );
				} catch (HyperDexClientException e)
				{
					e.printStackTrace();
				}
				
				synchronized(hclientFreeMonitor)
				{
					freeHClientQueue.add(HClinetFree);
					hclientFreeMonitor.notify();
				}
			}
			
			private JSONArray getGUIDsFromIterator(Iterator hyperdexResultIterator)
			{
				JSONArray guidJSON = new JSONArray();
				
				try
				{
					while( hyperdexResultIterator.hasNext() )
					{
						Object iterObj = hyperdexResultIterator.next();
						try
						{
							@SuppressWarnings("unchecked")
							Map<String, Object> wholeObjectMap = (Map<String, Object>) iterObj;
							String nodeGUID = wholeObjectMap.get(HYPERDEX_KEY_NAME).toString();
							guidJSON.put(nodeGUID);
						}
						catch (Exception ex)
						{
							ContextServiceLogger.getLogger().fine("object causing excp "+iterObj);
							ex.printStackTrace();
						}
					}
				} catch ( HyperDexClientException e )
				{
					e.printStackTrace();
				}
				return guidJSON;
			}
		}
}