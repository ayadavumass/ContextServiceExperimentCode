package edu.umass.cs.expcode;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNSReply;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.msocket.geocast.CSNodeConfig;
import edu.umass.cs.msocket.geocast.ContextServiceDemultiplexer;
import edu.umass.cs.msocket.geocast.MSocketGroupMember;
import edu.umass.cs.msocket.geocast.Utils;
import edu.umass.cs.msocket.gns.GNSCalls;


public class SimpleUpdateTest<Integer> implements InterfacePacketDemultiplexer
{
		//public static String csServerName 										= "ananas.cs.umass.edu";
		//public static int csPort 													= 5000;
		
		//public static final String configFileName									= "contextServiceNodeSetup.txt";
		
		//private static final HashMap<Integer, InetSocketAddress> nodeMap			= new HashMap<Integer, InetSocketAddress>();
	
		private static final Random rand 										= new Random();
		
		public static final String CLIENT_GUID_PREFIX							= "clientGUID";
		
		private static final HashMap<String, Double> attrValueMap				= new HashMap<String, Double>();
		
		//private static final ConcurrentLinkedQueue<Long>	outstandingReqs		= new ConcurrentLinkedQueue<Long>();
		
		//private static final int START_PORT										= 9189;
		// per 1000msec
		//public static final int NUMGUIDs											= 1;
		// prefix for client GUIDs clientGUID1, client GUID2, ...
		
		// stores the current values
		private final Integer myID;
		private final CSNodeConfig<Integer> csNodeConfig;
		private final JSONNIOTransport<Integer> niot;
		private final String sourceIP;
		private final int sourcePort;
		
		private final MSocketGroupMember mMember;
		
		//private int versionNum													= 0;
		
		public static int ATTR_UPDATE_RATE										= 5000;
		
		public static int NUMATTRs												= 100;
		
		public static final String CONTEXT_ATTR_PREFIX							= "context";
		public static final String REPLY_ADDR_KEY								= "ReplyAddress";
		
		public SimpleUpdateTest(Integer id) throws Exception
		{
			myID = id;
			
			String memberAlias = CLIENT_GUID_PREFIX+id;
			mMember = new MSocketGroupMember(memberAlias);
			
			sourcePort = 2000+rand.nextInt(50000);
			//START_PORT+Integer.parseInt(myID.toString());
			
			csNodeConfig = new CSNodeConfig<Integer>();
			
			sourceIP =  Utils.getActiveInterfaceInetAddresses().get(0).getHostAddress();
			
			ContextServiceLogger.getLogger().fine("Source IP address "+sourceIP);
			
			csNodeConfig.add(myID, new InetSocketAddress(sourceIP, sourcePort));
	        
	        AbstractPacketDemultiplexer pd = new ContextServiceDemultiplexer();
			
			ContextServiceLogger.getLogger().fine("\n\n node IP "+csNodeConfig.getNodeAddress(this.myID)+
					" node Port "+csNodeConfig.getNodePort(this.myID)+" nodeID "+this.myID);
			
			niot = new JSONNIOTransport<Integer>(this.myID,  csNodeConfig, pd , true);
			
			JSONMessenger<Integer> messenger = 
				new JSONMessenger<Integer>(niot.enableStampSenderInfo());
			
			pd.register(ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY, this);
			messenger.addPacketDemultiplexer(pd);
			
			GNSCalls.writeKeyValue(memberAlias, REPLY_ADDR_KEY, sourceIP+":"+sourcePort);
			
			for( int i=0;i<NUMATTRs;i++ )
	    	{
	    		attrValueMap.put("contextATT"+i, (double) 100);
	    		mMember.setAttributes("contextATT"+i, (double) 100);
	    	}
		}
		
		public void stopThis()
		{
			this.niot.stop();
		}
		
		public void handleUpdateReply(JSONObject jso)
		{
			//long time = System.currentTimeMillis();
			ValueUpdateFromGNSReply<Integer> vur;
			try 
			{
				vur = new ValueUpdateFromGNSReply<Integer>(jso);
				long currTime = System.currentTimeMillis();
				long tillContextTime = vur.getContextTime() - vur.getStartTime();
				long contextProcessTime = vur.getSendTime() - vur.getContextTime();
				long totalTime = currTime - vur.getStartTime();
				
				ContextServiceLogger.getLogger().fine
				("Update reply recvd tillContextTime"+tillContextTime+" contextProcessTime "+contextProcessTime+
						" totalTime "+totalTime);
				
				//Long start = outstandingReqs.poll();
				//if( start != null)
				//{
				//ContextServiceLogger.getLogger().fine("Update reply recvd "+(currTime-start)+" time from CS "+(currTime - vur.getSendTime()));
				//}
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
			//ContextServiceLogger.getLogger().fine("CONTEXTSERVICE EXPERIMENT: UPDATEFROMUSERREPLY REQUEST ID "
			//		+vur.getVersionNum()+" NUMATTR "+0+" AT "+time+" EndTime "
			//		+time);
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
		
		/**
		 * does the attribute updates at a rate.
		 * @throws JSONException 
		 * @throws IOException 
		 * @throws UnknownHostException 
		 */
		public void doAttributeUpdates() throws JSONException, UnknownHostException, IOException
		{
			for(int i=0;i<NUMATTRs;i++)
			{
				//ContextServiceLogger.getLogger().fine("doAttributeUpdates called "+i);
				String attName = "contextATT"+i;
				double nextVal = 1+rand.nextInt((int)(1500-1));
				
				double oldValue = attrValueMap.get(attName);
				attrValueMap.put(attName, nextVal);
				
				//outstandingReqs.add(System.currentTimeMillis());
				mMember.setAttributes(attName, nextVal);
				
				
				/*JSONObject allAttr = new JSONObject();
				for (String key : attrValueMap.keySet())
				{
					//ContextServiceLogger.getLogger().fine("doAttributeUpdates called "+i+" key "+key);
					allAttr.put(key, attrValueMap.get(key));
				}*/
				//ValueUpdateFromGNS<Integer> valMsg = new ValueUpdateFromGNS<Integer>(myID, versionNum++, guidString, attName, 
				//		oldValue+"", nextVal+"", allAttr, sourceIP, listenPort);
				//ContextServiceLogger.getLogger().fine("CONTEXTSERVICE EXPERIMENT: UPDATEFROMUSER REQUEST ID "
				//		+ valMsg.getVersionNum() +" AT "+System.currentTimeMillis());
				//Set<Integer> keySet= nodeMap.keySet();
				//int randIndex = rand.nextInt(keySet.size());
				//InetSocketAddress toMe = nodeMap.get(keySet.toArray()[randIndex]);
				//niot.sendToAddress(toMe, valMsg.toJSONObject());
				// just to add random wait.
				Random rand = new Random(System.currentTimeMillis());
				int wait = rand.nextInt(20);
				try 
				{
					Thread.sleep(wait);
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
		
		public static void main(String[] args) throws Exception
		{
			Integer clientID = Integer.parseInt(args[0]);
			ATTR_UPDATE_RATE = Integer.parseInt(args[1]);
			NUMATTRs = Integer.parseInt(args[2]);
			
			//int numReqs = Integer.parseInt(args[3]);
			//ContextServiceConfig.NUM_ATTRIBUTES = NUMATTRs;
			
			SimpleUpdateTest<Integer> basicObj 
										= new SimpleUpdateTest<Integer>(clientID);
			
			//String guidAlias = CLIENT_GUID_PREFIX+clientID;
			//String guidString = createGUID(clientID);
			
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
		}
		
		/**
		 * creates GUID and adds all attributes
		 */
		/*public static String createGUID(int clientID)
		{
			String guidAlias = CLIENT_GUID_PREFIX+clientID;
			String guidString = Utils.getSHA1(guidAlias).substring(0, 20);
			
			return guidString;
		}*/
		
		/*private void readNodeInfo() throws NumberFormatException, UnknownHostException, IOException
		{
			BufferedReader reader = new BufferedReader(new FileReader(configFileName));
			String line = null;
			
			while ((line = reader.readLine()) != null)
			{
				String [] parsed = line.split(" ");
				int readNodeId = Integer.parseInt(parsed[0])+CSTestConfig.startNodeID;
				InetAddress readIPAddress = InetAddress.getByName(parsed[1]);
				int readPort = Integer.parseInt(parsed[2]);
				
				nodeMap.put(readNodeId, new InetSocketAddress(readIPAddress, readPort));
			}
		}*/
}