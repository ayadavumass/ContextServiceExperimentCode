package edu.umass.cs.weatherExpClient;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.config.ContextServiceConfig.PrivacySchemes;


public class WeatherCaseStudyClient
{
	// 1% loss tolerance
	public static final double LOSS_TOLERANCE								= 0.01;
	
	//public static final String DUMMYGUID = "0B3C3AC6E25FF553BE3DC9176889E927C14CEA2A";
	public static final int MAX_QUERY_LEN 									= 8;
		
	// 100 seconds, experiment runs for 100 seconds
	public static final int EXPERIMENT_TIME									= 100000;
		
	// after sending all the requests it waits for 100 seconds 
	public static final int WAIT_TIME										= 100000;
		
	private String configFileName											= "contextServiceNodeSetup.txt";
	
	// decides whether to send query or update
	private static Random queryUpdateRand;
		
	private Random generalRand;
	
	// used for query workload.
	private Random queryRand;
	
	public static final String CLIENT_GUID_PREFIX							= "clientGUID";
	
	public static int NUMGUIDs												= 100;
	
	private static double currNumReqSent 									= 0;
	private static double currSearchReqSent 								= 0;
	
	private static double currNumReplyRecvd									= 0;
	
	//private static final HashMap<String, Double> attrValueMap				= new HashMap<String, Double>();
	
	public ExecutorService	 eservice										= null;
	
	// used in query work load, to keep track of current
	// number of attributes in the query
	private int currNumAttr;
	
	// used in update workload, to keep
	// track of next attribute to update.
	private int currentAttrNum;
	
	//private int numberRepliesRecvd										= 0;
	
	private long requestID													= 0;
	
	private final Object replyWaitMonitor									= new Object();
	private final Timer waitTimer;
		
	// used for generating predicate values in query
	private final Random queryGenRandom;
	
	private LinkedList<InetSocketAddress> nodeList							= null;
	
	private Random rand														= null;
	
	private ContextServiceClient csClient							= null;
	
	private int myID														= -1;
	//private JSONObject	attrValueObject									= null;
	
	private static int NUMATTRs;
	private static double requestsps;
	private static double queryUpdateRatio;
	
	public static final String CONTEXT_ATTR_PREFIX							= "context";
	public static final String REPLY_ADDR_KEY								= "ReplyAddress";
	public static String writerName;
	
	public static long expStartTime;
	
	private static String csHost;
	private static int csPort;
	
	public static final int NUM_QUERIES_IN_SYSTEM							= 1000;
	
		
	public static void main(String [] args) throws Exception
	{
		Integer clientID = Integer.parseInt(args[0]);
		writerName = "writer"+clientID;
		NUMATTRs = Integer.parseInt(args[1]);
		requestsps = Double.parseDouble(args[2]);
			
		// calculated by query/(query+update)
		queryUpdateRatio = Double.parseDouble(args[3]);
		
		NUMGUIDs = Integer.parseInt(args[4]);
		csHost = args[5];
		csPort = Integer.parseInt(args[6]);
		
		queryUpdateRand  = new Random(clientID);
			
		WeatherCaseStudyClient basicObj 
						= new WeatherCaseStudyClient(clientID);
			
			
		// should be less than 1000, for more more processes of this should be started
		//int waitInLoop = 1000/requestsps;
		long currTime = 0;
		expStartTime = System.currentTimeMillis();
		double numberShouldBeSentPerSleep = requestsps/10.0;
		
		
		while( ( (System.currentTimeMillis() - expStartTime) < EXPERIMENT_TIME ) )
		{
			for(int i=0;i<numberShouldBeSentPerSleep;i++)
			{
				sendAMessage(basicObj);
				currNumReqSent++;
			}
			currTime = System.currentTimeMillis();
			
			double timeElapsed = ((currTime- expStartTime)*1.0)/1000.0;
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
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (currNumReqSent * 1.0)/(timeInSec);
		System.out.println("Eventual sending rate "+sendingRate);
		
		basicObj.waitForFinish();
		//Thread.sleep(WAIT_TIME);
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (currNumReplyRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Result:Goodput "+sysThrput);
		
		System.exit(0);
	}
		
	private static void sendAMessage(WeatherCaseStudyClient basicObj)
	{
		basicObj.requestID++;
		double randVal = queryUpdateRand.nextDouble();
		
		// send query
		if( randVal < queryUpdateRatio )
		{
			currSearchReqSent++;
			System.out.println("Sending query requestID "+basicObj.requestID+" time "
			+System.currentTimeMillis());
			basicObj.sendQuery(basicObj.requestID);
			//basicObj.sendGet(basicObj.requestID);
		}
		// send update
		else
		{
			System.out.println
			("Sending update requestID "+basicObj.requestID+" time "+System.currentTimeMillis());
			basicObj.sendUpdate(basicObj.requestID);
		}
	}
		
	public WeatherCaseStudyClient(int id) throws Exception
	{
		nodeList = new LinkedList<InetSocketAddress>();
		
		readNodeInfo();
		this.myID = id;
		
		currNumAttr = 1;
		// attrname starts from 0
		currentAttrNum = 0;
		
		//queryHashMap  = new ConcurrentHashMap<Long, Long>();
		//updateHashMap = new ConcurrentHashMap<Long, Long>();
		
		rand = new Random((Integer)myID);
		
		waitTimer = new Timer();
		
		requestID = 0;
		
		queryRand = new Random((Integer)myID);
		generalRand = new Random((Integer)myID);
		//poissonRandom = new Random((Integer)myID);
		
		queryGenRandom = new Random((Integer)myID);
		
		csClient = new ContextServiceClient(csHost, csPort, false, 
				PrivacySchemes.NO_PRIVACY);
        
		//eservice = Executors.newFixedThreadPool(1000);
		eservice = Executors.newCachedThreadPool();
		
		//new Thread(new GoodPutThread()).start();
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
		String query = getQueryOfSize(currNumAttr);	
		eservice.execute(new SendingRequest(currID, SendingRequest.QUERY, query, currNumAttr, "", -1, -1, "") );
		currNumAttr = currNumAttr + 2;
	}
		
	/**
	 * This function sends update
	 */
	public void sendGet(long currID)
	{	
		int guidID = generalRand.nextInt(NUMGUIDs);
		
		String memberAlias = CLIENT_GUID_PREFIX+myID;
		String realAlias = memberAlias+guidID;
		String myGUID = getSHA1(realAlias);
		
		eservice.execute(new SendingRequest(currID, SendingRequest.GET, "", -1, "", -1, -1, myGUID) );
	}
		
	/**
	 * This function sends update
	 */
	public void sendUpdate(long currID)
	{
		if( currentAttrNum >= NUMATTRs )
		{
			currentAttrNum = 0;
		}
		
		String attName = "contextATT"+currentAttrNum;
		double nextVal = 1+generalRand.nextInt((int)(1500-1));
		double oldVal = Double.MIN_VALUE;
		
		int guidID = generalRand.nextInt(NUMGUIDs);
		
		String memberAlias = CLIENT_GUID_PREFIX+myID;
		String realAlias = memberAlias+guidID;
		String myGUID = getSHA1(realAlias);
		
		eservice.execute(new SendingRequest
				(currID, SendingRequest.UPDATE, "", -1, attName, nextVal, oldVal, myGUID) );
		
		currentAttrNum++;
	}	
		
	private void waitForFinish()
	{
		waitTimer.schedule(new WaitTimerTask(), WAIT_TIME);
		
		//while( currNumReplyRecvd < currNumReqSent )
		while( !checkForCompletionWithLossTolerance() )
		{
			synchronized(this.replyWaitMonitor)
			{
				try
				{
					this.replyWaitMonitor.wait();
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
		stopThis();	
		waitTimer.cancel();
		//System.exit(0);
	}
		
	public class WaitTimerTask extends TimerTask
	{
		@Override
		public void run()
		{
			// print the remaining update and query times
			// and finish the process, cancel the timer and exit JVM.
			stopThis();
			
			double endTimeReplyRecvd = System.currentTimeMillis();
			double sysThrput= (currNumReplyRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
			
			System.out.println("Result:TimeOutThroughput "+sysThrput);
			
			waitTimer.cancel();
			// just terminate the JVM
			System.exit(0);
		}
	}
	
	public void stopThis()
	{
		this.eservice.shutdownNow();
	}
		
	private boolean checkForCompletionWithLossTolerance()
	{
		boolean completion = false;
		
		double withinLoss = (LOSS_TOLERANCE * currNumReqSent)/100.0;
		if( (currNumReqSent - currNumReplyRecvd) <= withinLoss )
		{
			completion = true;
		}
		return completion;
	}
		
//	private void handleRefreshTrigger(JSONObject jso)
//	{
//		try
//		{
//			RefreshTrigger<Integer> qmur;
//			qmur = new RefreshTrigger<Integer>(jso);
//			
//			long reqID = qmur.getVersionNum();
//			
//			System.out.println("RefreshTrigger completion requestID "+reqID+" time "+System.currentTimeMillis());
//		} catch (JSONException e)
//		{
//			e.printStackTrace();
//		}
//	}
		
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
       String returnGUID = sb.toString();
       return returnGUID.substring(0, 40);
	}
		
	public String getQueryOfSize(int queryLength)
	{
	    String query="";
	    for(int i=0;i<queryLength;i++)
	    {
	    	int attrNum = queryRand.nextInt(NUMATTRs);
	    	int beg = this.queryGenRandom.nextInt(1400);
	    	int end = beg+this.queryGenRandom.nextInt(1500 - beg-3);
	    	
		/*end = beg+15;
	        if( (beg < 750) && (end >= 750) )
	        {
	        	beg = this.queryGenRandom.nextInt(730);
	        	end = beg+15;
	        }*/
		
		String predicate = beg+" <= contextATT"+attrNum+" <= "+end;
	    	//String predicate = 1+" <= contextATT"+attrNum+" <= "+1500;
	    	if(i==0)
	    	{
	    		query = predicate;
	    	}
	    	else
	    	{
	    		query = query + " && "+predicate;
	    	}
	    }
	    return query;
	}
		
	private void readNodeInfo() throws NumberFormatException, UnknownHostException, IOException
	{
		FileReader fread = new FileReader(configFileName);
		BufferedReader reader = new BufferedReader(fread);
		String line = null;
		
		while ( (line = reader.readLine()) != null )
		{
			String [] parsed = line.split(" ");
			InetAddress readIPAddress = InetAddress.getByName(parsed[1]);
			int readPort = Integer.parseInt(parsed[2]);
			
			nodeList.add(new InetSocketAddress(readIPAddress, readPort));
		}
		reader.close();
		fread.close();
	}
		
	private InetSocketAddress getRandomNodeSock()
	{
		int size = nodeList.size();
		return nodeList.get( rand.nextInt(size) );
	}
	
	/*@Override
	public boolean handleMessage(JSONObject jsonObject)
	{
		try
		{
			if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE) 
					== ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY.getInt() )
			{
				handleQueryReply(jsonObject);
			} else if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
					== ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY.getInt() )
			{
				handleUpdateReply(jsonObject);
			} else if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
					== ContextServicePacket.PacketType.REFRESH_TRIGGER.getInt() )
			{
				//handleRefreshTrigger(jsonObject);
			} else if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
					== ContextServicePacket.PacketType.GET_REPLY_MESSAGE.getInt() )
			{
				handleGetReply(jsonObject);
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return true;
	}*/
	
	public class SendingRequest implements Runnable
	{
		public static final int UPDATE = 1;
		public static final int QUERY  = 2;
		public static final int GET    = 3;
		
		private final int reqType;
		private final String query;
		private final int numAttr;
		private final String attrName;
		private final double nextVal;
		private final long currID;
		private final double oldVal;
		private final String GUID;
		
		public SendingRequest(long currID, int reqType, String query, int numAttr, 
				String attrName, double nextVal, double oldVal, String GUID)
		{
			this.reqType = reqType;
			this.query = query;
			this.numAttr = numAttr;
			this.attrName = attrName;
			this.nextVal = nextVal;
			this.currID = currID;
			this.oldVal = oldVal;
			this.GUID = GUID;
		}
		
		@Override
		public void run()
		{
			switch(reqType)
			{
				case QUERY:
				{
					//ContextServiceLogger.getLogger().fine("JointRequestsWithMissPoisson:QUERY "+query+" numAttr "+numAttr);
					try 
					{
						sendQueryToContextService(query, currID);
					} catch (IOException e) 
					{
						e.printStackTrace();
					} catch (JSONException e) 
					{
						e.printStackTrace();
					}
					break;
				}
				case UPDATE:
				{
					//ContextServiceLogger.getLogger().fine("JointRequestsWithMissPoisson:UPDATE "+attrName+" "+nextVal);
					//mMember.setAttributes(attrName, nextVal);
					//ContextServiceLogger.getLogger().fine("Value update sent updID "+currID+" time "+System.currentTimeMillis());
					sendUpdateToContextService(currID, attrName, oldVal, nextVal, GUID);
					break;
				}
				case GET:
				{
					sendGetToContextService(currID, GUID);
					break;
				}
			}
		}
			
		private void sendQueryToContextService(String query, long userReqNum) throws IOException, JSONException
		{
			JSONArray resultArray = new JSONArray();
			long expiryTime = 300000;
			
			csClient.sendSearchQuery(query, resultArray, expiryTime);
			
			System.out.println( "Query completion requestID "+userReqNum+" time "+System.currentTimeMillis()+
					" replySize "+resultArray.length() );
			
			synchronized(replyWaitMonitor)
			{
				currNumReplyRecvd++;
				
				//if(currNumReplyRecvd == currNumReqSent)
				if( checkForCompletionWithLossTolerance() )
				{
					replyWaitMonitor.notify();
				}
			}
		}
			
		public void sendUpdateToContextService(long versionNum, String attrName, double oldVal, double newVal, String GUID)
		{
			try
			{
				JSONObject attrValuePair = new JSONObject();
				attrValuePair.put(attrName, newVal);
				
				csClient.sendUpdate(GUID, null, 
						attrValuePair, versionNum);
//				ValueUpdateFromGNS<Integer> valUpdFromGNS = 
//						new ValueUpdateFromGNS<Integer>(myID, versionNum, GUID, attrValuePair, sourceIP, sourcePort, versionNum );
//				
//				niot.sendToAddress(getRandomNodeSock(), valUpdFromGNS.toJSONObject());
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		public void sendGetToContextService(long versionNum, String GUID)
		{
//			try
//			{
//				GetMessage<Integer> getMessageObj = 
//						new GetMessage<Integer>(myID, versionNum, GUID,  sourceIP, sourcePort );
//				
//				niot.sendToAddress(getRandomNodeSock(), getMessageObj.toJSONObject());
//			} catch (JSONException e)
//			{
//				e.printStackTrace();
//			} catch (UnknownHostException e)
//			{
//				e.printStackTrace();
//			} catch (IOException e)
//			{
//				e.printStackTrace();
//			}
			
			JSONObject jsoObject = csClient.sendGetRequest(GUID);
			
			System.out.println("Query completion requestID "+versionNum+
					" time "+System.currentTimeMillis()+" size "+jsoObject.length());
			
			synchronized(replyWaitMonitor)
			{
				currNumReplyRecvd++;
				
				//if(currNumReplyRecvd == currNumReqSent)
				if(checkForCompletionWithLossTolerance())
				{
					replyWaitMonitor.notify();
				}
			}	
		}
	}
		
	/*private class GoodPutThread implements Runnable
	{
		private double lastNumReplyRecvd = 0;
		public GoodPutThread()
		{
		}
		
		@Override
		public void run()
		{
			while(true)
			{
				try
				{
					Thread.sleep(10000);
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
				double goodput = ((currNumReplyRecvd - lastNumReplyRecvd)*1.0)/10.0;
				lastNumReplyRecvd = currNumReplyRecvd;
				System.out.println("Goodput: "+goodput);
			}
		}
	}*/
	
	/*public void handleUpdateReply(JSONObject jso)
	{
		ValueUpdateFromGNSReply<Integer> vur;
		try
		{
			vur = new ValueUpdateFromGNSReply<Integer>(jso);
			long currReqID = vur.getVersionNum();
			
			System.out.println("Update completion requestID "+currReqID+" time "+System.currentTimeMillis());
			
			synchronized(this.replyWaitMonitor)
			{
				currNumReplyRecvd++;
				if(currNumReplyRecvd == currNumReqSent)
				{
					this.replyWaitMonitor.notify();
				}
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
		
	private void handleQueryReply(JSONObject jso)
	{
		try
		{
			QueryMsgFromUserReply<Integer> qmur;
			qmur = new QueryMsgFromUserReply<Integer>(jso);
			
			long reqID = qmur.getUserReqNum();
			int resultSize = qmur.getReplySize();
//			r(int i=0;i<qmur.getResultGUIDs().length();i++)
//			{
//				resultSize = resultSize+qmur.getResultGUIDs().getJSONArray(i).length();
//			}
			System.out.println( "Query completion requestID "+reqID+" time "+System.currentTimeMillis()+
					" replySize "+resultSize );
			
			synchronized(this.replyWaitMonitor)
			{
				currNumReplyRecvd++;
				
				//if(currNumReplyRecvd == currNumReqSent)
				if( checkForCompletionWithLossTolerance() )
				{
					this.replyWaitMonitor.notify();
				}
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	private void handleGetReply(JSONObject jso)
	{
		try
		{
			GetReplyMessage<Integer> getReply= new GetReplyMessage<Integer>(jso);
			
			long reqID = getReply.getReqID();
			
			System.out.println("Query completion requestID "+reqID+" time "+System.currentTimeMillis()+" size "+getReply.getGUIDObject().length());
	
			synchronized(this.replyWaitMonitor)
			{
				currNumReplyRecvd++;
				
				//if(currNumReplyRecvd == currNumReqSent)
				if(checkForCompletionWithLossTolerance())
				{
					this.replyWaitMonitor.notify();
				}
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}*/
}