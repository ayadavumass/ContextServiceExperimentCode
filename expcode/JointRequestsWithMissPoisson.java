package edu.umass.cs.expcode;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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


public class JointRequestsWithMissPoisson<NodeIDType> implements InterfacePacketDemultiplexer
{
	    // 100 seconds, experiment runs for 100 seconds
		public static final int EXPERIMENT_TIME									= 100000;
		
		// after sending all the requests it waits for 100 seconds 
		public static final int WAIT_TIME										= 100000;
		
		public static final int TOTAL_NUM_REQS									= 4000;
		
		// decides whether to send query or update
		private static Random queryUpdateRand;
		
		private Random generalRand;
		
		// used for query workload.
		private Random queryRand;
		
		public static final String CLIENT_GUID_PREFIX							= "clientGUID";
		
		private static final HashMap<String, Double> attrValueMap				= new HashMap<String, Double>();
		
		// stores the current values
		private final NodeIDType myID;
		private final CSNodeConfig<NodeIDType> csNodeConfig;
		private final JSONNIOTransport<NodeIDType> niot;
		private final JSONMessenger<NodeIDType> messenger;
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
		
		private int requentID;
		private final Object repliedRecvMonitor									= new Object();
		
		private final Object finishMonitor											= new Object();
		
		private final ConcurrentHashMap<Integer, Long> queryHashMap;
		private final ConcurrentLinkedQueue<Long> updateQueue;
		
		private final Timer waitTimer;
		
		//waiting time for poisson process.
		private final Random poissonRandom;
		
		// used for generating predicate values in query
		private final Random queryGenRandom;
		
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
			
			
			JointRequestsWithMissPoisson<Integer> basicObj 
										= new JointRequestsWithMissPoisson<Integer>(clientID);
			
			// should be less than 1000, for more more processes of this should be started
			//int waitInLoop = 1000/requestsps;
			int numReqs  = 0;
			int currTime = 0;
			
			while( ( numReqs <  TOTAL_NUM_REQS ) && ( currTime < EXPERIMENT_TIME ) )
			{
				basicObj.requentID++;
				double randVal = queryUpdateRand.nextDouble();
				
				// send query
				if( randVal < queryUpdateRatio )
				{
					basicObj.queryHashMap.put( basicObj.requentID, System.currentTimeMillis() );
					basicObj.sendQuery(basicObj.requentID);
				}
				// send update
				else
				{
					basicObj.updateQueue.add(System.currentTimeMillis());
					
					basicObj.sendUpdate(basicObj.requentID);
				}
				
				long waitInLoop = basicObj.getInterTimePoisson();
				Thread.sleep(waitInLoop);
				currTime += waitInLoop;
				numReqs++;
			}
			
			basicObj.waitForFinish();
		}
		
		
		private long getInterTimePoisson()
		{
			double rate = requestsps;
			
			double interTime = -Math.log(1-poissonRandom.nextDouble()) / rate;
			long returnVal = (long) (interTime*1000);
			return returnVal;
		}
		
		private void waitForFinish() 
		{
			waitTimer.schedule(new WaitTimerTask(), WAIT_TIME);
			
			while(this.numberRepliesRecvd < TOTAL_NUM_REQS)
			{
				synchronized(this.finishMonitor)
				{
					try 
					{
						this.finishMonitor.wait();
					} catch (InterruptedException e) 
					{
						e.printStackTrace();
					}
				}
			}
			
			try
			{
				finish();
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			waitTimer.cancel();
			System.exit(0);
		}
		
		
		private void finish() throws InterruptedException
		{
			stopThis();
			DefaultGNSClient.gnsClient.stop();
			ContextServiceCallsSingleton.stopThis();
		}
		
		public JointRequestsWithMissPoisson(NodeIDType id) throws Exception
		{
			myID = id;
			
			currNumAttr = 1;
			// attrname starts from 0
			currentAttrNum = 0;
			
			queryHashMap = new ConcurrentHashMap<Integer, Long>();
			updateQueue = new ConcurrentLinkedQueue<Long>();
			
			waitTimer = new Timer();
			
			requentID = 0;
			
			queryRand = new Random((Integer)myID);
			generalRand = new Random();
			poissonRandom = new Random((Integer)myID);
			
			queryGenRandom = new Random((Integer)myID);
			
			String memberAlias = CLIENT_GUID_PREFIX+id;
			mMember = new MSocketGroupMember(memberAlias);
			
			sourcePort = 2000+generalRand.nextInt(50000);
			//START_PORT+Integer.parseInt(myID.toString());
			
			csNodeConfig = new CSNodeConfig<NodeIDType>();
			
			sourceIP =  Utils.getActiveInterfaceInetAddresses().get(0).getHostAddress();
			
			ContextServiceLogger.getLogger().fine("Source IP address "+sourceIP);
			
			csNodeConfig.add(myID, new InetSocketAddress(sourceIP, sourcePort));
	        
	        AbstractPacketDemultiplexer pd = new ContextServiceDemultiplexer();
			
			ContextServiceLogger.getLogger().fine("\n\n node IP "+csNodeConfig.getNodeAddress(this.myID) +
					" node Port "+csNodeConfig.getNodePort(this.myID)+" nodeID "+this.myID);
			
			niot = new JSONNIOTransport<NodeIDType>(this.myID,  csNodeConfig, pd , true);
			
			messenger = 
				new JSONMessenger<NodeIDType>(niot.enableStampSenderInfo());
			
			pd.register(ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY, this);
			messenger.addPacketDemultiplexer(pd);
			
			eservice = Executors.newFixedThreadPool(1000);
			
			GNSCalls.writeKeyValue(memberAlias, REPLY_ADDR_KEY, sourceIP+":"+sourcePort);
		}
		
		/**
		 * Depending on the random outcome this function sends query
		 */
		public void sendQuery(int currID)
		{
			if( currNumAttr > 30 )
			{
				currNumAttr = 1;
			}
			String query = getQueryOfSize(currNumAttr);
			
			eservice.execute(new SendingRequest(currID, SendingRequest.QUERY, query, currNumAttr, "", -1) );
			currNumAttr = currNumAttr + 2;
		}
		
		/**
		 * this function sends update
		 */
		public void sendUpdate(int currID)
		{
			if( currentAttrNum >= NUMATTRs )
			{
				currentAttrNum = 0;
			}
			
			String attName = "contextATT"+currentAttrNum;
			double nextVal = 1+generalRand.nextInt((int)(1500-1));
			
			eservice.execute(new SendingRequest(currID, SendingRequest.UPDATE, "", -1, attName, nextVal) );
			
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
			ValueUpdateFromGNSReply<NodeIDType> vur;
			try
			{
				vur = new ValueUpdateFromGNSReply<NodeIDType>(jso);
				long currTime = System.currentTimeMillis();
				long tillContextTime = vur.getContextTime() - vur.getStartTime();
				long contextProcessTime = vur.getSendTime() - vur.getContextTime();
				long totalTime = currTime - vur.getStartTime();
				
				// just to remove item
				this.updateQueue.poll();
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
		    	int beg = this.queryGenRandom.nextInt(1400);
		    	int end = beg+this.queryGenRandom.nextInt(1500 - beg-3); 	
		    	
		    	
		    	String predicate = beg+" <= contextATT"+attrNum+" <= "+end;
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
		
		public class WaitTimerTask extends TimerTask
		{
			@Override
			public void run()
			{
				// print the remaining update and query times
				// and finish the process, cancel the timer and exit JVM.
				try 
				{
					finish();
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
				long now = System.currentTimeMillis();
				
				//Iterator hashIterator = 
				for( int reqID: queryHashMap.keySet() )
				{
					ContextServiceLogger.getLogger().fine("TimeOutMSOCKETWRITERINTERNAL from CS querytime "
						+(now-queryHashMap.get(reqID)) +" numAttr "+0+" cstime "+(now-queryHashMap.get(reqID)));
					
					//ContextServiceLogger.getLogger().fine(key  +" :: "+ studentGrades.get(key));
					//if you uncomment below code, it will throw java.util.ConcurrentModificationException
					//studentGrades.remove("Alan");
				}
				queryHashMap.clear();
				
				while( !updateQueue.isEmpty() )
				{
					Long startTime = updateQueue.poll();
					if( startTime != null )
					{
						ContextServiceLogger.getLogger().fine
						("TimeOutUpdate reply recvd tillContextTime"+(now-startTime)+" contextProcessTime "+(now-startTime)+
								" totalTime "+(now-startTime));
					}
				}
				
				waitTimer.cancel();
				
				// just terminate the JVM
				System.exit(0);
			}
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
			private final int currID;
			
			public SendingRequest(int currID, int reqType, String query, int numAttr, String attrName, double nextVal)
			{
				this.reqType = reqType;
				this.query = query;
				this.numAttr = numAttr;
				this.attrName = attrName;
				this.nextVal = nextVal;
				this.currID = currID;
			}
			
			@Override
			public void run()
			{
				switch(reqType)
				{
					case QUERY:
					{
						ContextServiceLogger.getLogger().fine("JointRequestsWithMissPoisson:QUERY "+query+" numAttr "+numAttr);
						sendQueryToContextService(query, numAttr);
						break;
					}
					case UPDATE:
					{
						ContextServiceLogger.getLogger().fine("JointRequestsWithMissPoisson:UPDATE "+attrName+" "+nextVal);
						mMember.setAttributes(attrName, nextVal);
						break;
					}
				}
			}
			
			public void sendQueryToContextService(String query, int numAttr)
			{
				try
				{	
					long startTime = System.currentTimeMillis();
					MSocketGroupWriter testWrit = new MSocketGroupWriter(writerName, query);
					long endTime = System.currentTimeMillis();
					ContextServiceLogger.getLogger().fine("CONTEXTSERVICE EXPERIMENT: QUERYTIME NUMATTR "+numAttr+" TIME "+(endTime-startTime)+" Answer ");
					testWrit.printGroupMembers();
					
					queryHashMap.remove(currID);
					
					synchronized(repliedRecvMonitor)
					{
						numberRepliesRecvd++;
						
						if( numberRepliesRecvd >= TOTAL_NUM_REQS )
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