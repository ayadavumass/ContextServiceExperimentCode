package edu.umass.cs.expcode;
import java.net.InetSocketAddress;
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
import edu.umass.cs.msocket.geocast.Utils;
import edu.umass.cs.msocket.gns.DefaultGNSClient;
import edu.umass.cs.msocket.gns.GNSCalls;


public class DummyGUIDStorage<NodeIDType> implements InterfacePacketDemultiplexer
{
	public static final int NUMGUIDs											= 10;
	
	// decides whether to send query or update
	//private static Random queryUpdateRand;
	
	private Random generalRand;
	
	// used for query workload.
	//private Random queryRand;
	
	public static final String CLIENT_GUID_PREFIX							= "clientGUID";
	
	//private static final HashMap<String, Double> attrValueMap					= new HashMap<String, Double>();
	
	// stores the current values
	private final NodeIDType myID;
	private final CSNodeConfig<NodeIDType> csNodeConfig;
	private final JSONNIOTransport<NodeIDType> niot;
	private final JSONMessenger<NodeIDType> messenger;
	private final String sourceIP;
	private final int sourcePort;
	
	private final MSocketGroupMember[] mMembers = new MSocketGroupMember[NUMGUIDs];
	
	
	public ExecutorService	 eservice											= null;
	
	private static int NUMATTRs;
	//private static int requestsps;
	//private static double queryUpdateRatio;
	
	public static final String CONTEXT_ATTR_PREFIX							= "context";
	public static final String REPLY_ADDR_KEY								= "ReplyAddress";
	public static String writerName;
	
	
	public static void main(String [] args) throws Exception
	{
		Integer clientID = Integer.parseInt(args[0]);
		writerName = "writer"+clientID;
		NUMATTRs = Integer.parseInt(args[1]);
		
		DummyGUIDStorage<Integer> basicObj 
									= new DummyGUIDStorage<Integer>(clientID);
		
		for(int i=0; i<NUMGUIDs; i++)
		{
			basicObj.executeUpdateForGUID(i);
		}
		
		// wait for 100 secs
		Thread.sleep(200000);
		
		try
		{
			basicObj.finish();
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		
		System.exit(0);
	}
	
	private void finish() throws InterruptedException
	{
		stopThis();
		DefaultGNSClient.gnsClient.stop();
		ContextServiceCallsSingleton.stopThis();
	}
	
	public DummyGUIDStorage(NodeIDType id) throws Exception
	{
		myID = id;
		
		generalRand = new Random(System.currentTimeMillis());
		
		sourcePort = 2000+generalRand.nextInt(50000);
		
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
		
		eservice = Executors.newFixedThreadPool(100);
	}
	
	private void executeUpdateForGUID( int id )
	{
		eservice.execute( new UpdateGUID(id) );
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
	
	public class UpdateGUID implements Runnable
	{
		private final int id;
		
		public UpdateGUID(int id)
		{
			this.id = id;
		}
		
		@Override
		public void run()
		{
			try 
			{
				String memberAlias = CLIENT_GUID_PREFIX+myID;
				String realAlias = memberAlias+id;
				
				mMembers[id] = new MSocketGroupMember(realAlias);
				GNSCalls.writeKeyValue(realAlias, REPLY_ADDR_KEY, sourceIP+":"+sourcePort);
				
				Random rand = new Random();
				for(int i=0; i<NUMATTRs; i++)
				{
					mMembers[id].setAttributes( "contextATT"+i, 1+rand.nextInt(1498) );
					
					try
					{
						Thread.sleep(1000);
					} catch (InterruptedException e)
					{
						e.printStackTrace();
					}
				}
				
				mMembers[id].close();
			} catch (Exception e1)
			{
				e1.printStackTrace();
			}
		}
	}
	
}