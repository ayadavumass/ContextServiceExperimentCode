package edu.umass.cs.expcode;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNSReply;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.msocket.geocast.CSNodeConfig;
import edu.umass.cs.msocket.geocast.ContextServiceCallsSingleton;
import edu.umass.cs.msocket.geocast.ContextServiceDemultiplexer;
import edu.umass.cs.msocket.geocast.MSocketGroupMember;
import edu.umass.cs.msocket.geocast.MSocketGroupWriter;
import edu.umass.cs.msocket.geocast.Utils;
import edu.umass.cs.msocket.gns.DefaultGNSClient;
import edu.umass.cs.msocket.gns.GNSCalls;


/**
 * sends both context service query requests and updates depending on the ratio.
 * Takes ratio and number of requests/sec as input.
 * @author adipc
 */
public class JointRequests<Integer> implements InterfacePacketDemultiplexer
{
	public static final int TOTAL_NUM_REQS									= 4000;
	
	// decides whether to send query or update
	private static Random queryUpdateRand;
	
	private Random generalRand;
	
	// used for query workload.
	private Random queryRand;
	
	public static final String CLIENT_GUID_PREFIX							= "clientGUID";
	
	private static final HashMap<String, Double> attrValueMap				= new HashMap<String, Double>();
	
	// stores the current values
	private final Integer myID;
	private final CSNodeConfig<Integer> csNodeConfig;
	private final JSONNIOTransport<Integer> niot;
	private final JSONMessenger<Integer> messenger;
	private final String sourceIP;
	private final int sourcePort;
	
	private final MSocketGroupMember mMember;
	
	
	public ExecutorService	 eservice											= null;
	
	// used in query work load, to keep track of current
	// number of attributes in the query
	private int currNumAttr;
	
	// used in update workload, to keep
	// track of next attribute to update.
	private int currentAttrNum;
	
	private int numberRepliesRecvd												= 0;
	private final Object repliedRecvMonitor									= new Object();
	
	private final Object finishMonitor											= new Object();
	
	//private int versionNum													= 0;
	
	//public static int ATTR_UPDATE_RATE										= 5000;
	
	private static int NUMATTRs;
	private static int requestsps;
	private static double queryUpdateRatio;
	
	public static final String CONTEXT_ATTR_PREFIX							= "context";
	public static final String REPLY_ADDR_KEY								= "ReplyAddress";
	public static String writerName;
	
	public static void main(String [] args) throws Exception
	{
		Integer clientID = Integer.parseInt(args[0]);
		writerName = "writer"+clientID;
		NUMATTRs = Integer.parseInt(args[1]);
		requestsps = Integer.parseInt(args[2]);
		
		// calculated by query/(query+update)
		queryUpdateRatio = Double.parseDouble(args[3]);
		
		queryUpdateRand = new Random(clientID);
		
		//ATTR_UPDATE_RATE = Integer.parseInt(args[1]);
		//NUMATTRs = Integer.parseInt(args[2]);
		//int numReqs = Integer.parseInt(args[3]);
		//ContextServiceConfig.NUM_ATTRIBUTES = NUMATTRs;
		
		JointRequests<Integer> basicObj 
									= new JointRequests<Integer>(clientID);
		
		// should be less than 1000, for more more processes of this should be started
		int waitInLoop = 1000/requestsps;
		int numReqs = 0;
		while( numReqs <  TOTAL_NUM_REQS )
		{
			double randVal = queryUpdateRand.nextDouble();
			
			// send query
			if( randVal < queryUpdateRatio )
			{
				basicObj.sendQuery();
			}
			// send update
			else
			{
				basicObj.sendUpdate();
			}
			Thread.sleep(waitInLoop);
			numReqs++;
		}
		
		basicObj.finish();
		ContextServiceLogger.getLogger().fine("ALL Replies recvd");
		
		//String guidAlias = CLIENT_GUID_PREFIX+clientID;
		//String guidString = createGUID(clientID);
	}
	
	private void finish() throws InterruptedException
	{
		while(this.numberRepliesRecvd < TOTAL_NUM_REQS)
		{
			synchronized(this.finishMonitor)
			{
				this.finishMonitor.wait();
			}
		}
		
		stopThis();
		DefaultGNSClient.gnsClient.stop();
		ContextServiceCallsSingleton.stopThis();
	}
	
	/*public static void main(String[] args) throws Exception
	{
		while(true)
		{
			try
			{
				basicObj.doAttributeUpdates();
				
				try
				{
					Thread.sleep(100000/ATTR_UPDATE_RATE);
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
	}*/
	
	public JointRequests(Integer id) throws Exception
	{
		myID = id;
		
		currNumAttr = 1;
		// attrname starts from 0
		currentAttrNum = 0;
		
		queryRand = new Random((Integer)myID);
		generalRand = new Random();
		
		String memberAlias = CLIENT_GUID_PREFIX+id;
		mMember = new MSocketGroupMember(memberAlias);
		
		sourcePort = 2000+generalRand.nextInt(50000);
		//START_PORT+Integer.parseInt(myID.toString());
		
		csNodeConfig = new CSNodeConfig<Integer>();
		
		sourceIP =  Utils.getActiveInterfaceInetAddresses().get(0).getHostAddress();
		
		ContextServiceLogger.getLogger().fine("Source IP address "+sourceIP);
		
		csNodeConfig.add(myID, new InetSocketAddress(sourceIP, sourcePort));
        
        AbstractPacketDemultiplexer pd = new ContextServiceDemultiplexer();
		
		ContextServiceLogger.getLogger().fine("\n\n node IP "+csNodeConfig.getNodeAddress(this.myID) +
				" node Port "+csNodeConfig.getNodePort(this.myID)+" nodeID "+this.myID);
		
		niot = new JSONNIOTransport<Integer>(this.myID,  csNodeConfig, pd , true);
		
		messenger = 
			new JSONMessenger<Integer>(niot.enableStampSenderInfo());
		
		pd.register(ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY, this);
		messenger.addPacketDemultiplexer(pd);
		
		eservice = Executors.newFixedThreadPool(1000);
		
		GNSCalls.writeKeyValue(memberAlias, REPLY_ADDR_KEY, sourceIP+":"+sourcePort);
		
		/*for( int i=0;i<NUMATTRs;i++ )
    	{
    		attrValueMap.put("contextATT"+i, (double) 100);
    		mMember.setAttributes("contextATT"+i, (double) 100);
    	}*/
	}
	
	/**
	 * Depending on the random outcome this function sends query
	 */
	public void sendQuery()
	{
		//int startNumAttr = 1;
		if( currNumAttr > 30 )
		{
			currNumAttr = 1;
		}
		String query = getQueryOfSize(currNumAttr);
		
		eservice.execute(new SendingRequest(SendingRequest.QUERY, query, currNumAttr, "", -1));
		currNumAttr = currNumAttr + 2;
	}
	
	/**
	 * this function sends update
	 */
	public void sendUpdate()
	{
		//currentAttrNum
		//for(int i=0;i<NUMATTRs;i++)
		//{
		if( currentAttrNum >= NUMATTRs )
		{
			currentAttrNum = 0;
		}
		String attName = "contextATT"+currentAttrNum;
		double nextVal = 1+generalRand.nextInt((int)(1500-1));
			
		//mMember.setAttributes(attName, nextVal);
		eservice.execute(new SendingRequest(SendingRequest.UPDATE, "", -1, attName, nextVal));
		
		currentAttrNum++;
	}
	
	public void stopThis()
	{
		this.eservice.shutdownNow();
		this.niot.stop();
		this.messenger.stop();
	}
	
	public void handleUpdateReply(JSONObject jso)
	{
		ValueUpdateFromGNSReply<Integer> vur;
		try
		{
			vur = new ValueUpdateFromGNSReply<Integer>(jso);
			long currTime = System.currentTimeMillis();
			long tillContextTime = vur.getContextTime() - vur.getStartTime();
			long contextProcessTime = vur.getSendTime() - vur.getContextTime();
			long totalTime = currTime - vur.getStartTime();
			
			synchronized(repliedRecvMonitor)
			{
				numberRepliesRecvd++;
				
				if( numberRepliesRecvd >= TOTAL_NUM_REQS)
				{
					synchronized(finishMonitor)
					{
						finishMonitor.notify();
					}
				}
			}
			
			ContextServiceLogger.getLogger().fine
			("Update reply recvd tillContextTime"+tillContextTime+" contextProcessTime "+contextProcessTime+
					" totalTime "+totalTime);
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean handleJSONObject(JSONObject jsonObject)
	{
		ContextServiceLogger.getLogger().fine("QuerySourceDemux JSON packet recvd "+jsonObject);
		try
		{
			if(jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
					== ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY.getInt())
			{
				//ContextServiceLogger.getLogger().fine("JSON packet recvd "+jsonObject);
				handleUpdateReply(jsonObject);
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return true;
	}
	
	public String getQueryOfSize(int queryLength)
	{
		String query="";
	    for(int i=0;i<queryLength;i++)
	    {
	    	int attrNum = queryRand.nextInt(NUMATTRs);
	    	
	    	String predicate = "1 <= contextATT"+attrNum+" <= 2";
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
	
	public class SendingRequest implements Runnable
	{
		public static final int UPDATE = 1;
		public static final int QUERY  = 2;
		
		private final int reqType;
		private final String query;
		private final int numAttr;
		private final String attrName;
		private final double nextVal;
		
		public SendingRequest(int reqType, String query, int numAttr, String attrName, double nextVal)
		{
			this.reqType = reqType;
			this.query = query;
			this.numAttr = numAttr;
			this.attrName = attrName;
			this.nextVal = nextVal;
		}
		
		@Override
		public void run()
		{
			switch(reqType)
			{
				case QUERY:
				{
					sendQueryToContextService(query, numAttr);
					break;
				}
				case UPDATE:
				{
					mMember.setAttributes(attrName, nextVal);
					break;
				}
			}
		}
		
		public void sendQueryToContextService(String query, int numAttr)
		{
			try
			{
				ContextServiceLogger.getLogger().fine("Sending query "+query+" numAttr "+numAttr);
				
				//int userReqNum = requestCounter++;
				long startTime = System.currentTimeMillis();
				MSocketGroupWriter testWrit = new MSocketGroupWriter(writerName, query);
				long endTime = System.currentTimeMillis();
				//ContextServiceLogger.getLogger().fine("CONTEXTSERVICE EXPERIMENT: QUERYTIME NUMATTR "+numAttr+" TIME "+(endTime-startTime));
				
				synchronized(repliedRecvMonitor)
				{
					numberRepliesRecvd++;
					
					if( numberRepliesRecvd >= TOTAL_NUM_REQS)
					{
						synchronized(finishMonitor)
						{
							finishMonitor.notify();
						}
					}
				}
				testWrit.close();
			} catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
}