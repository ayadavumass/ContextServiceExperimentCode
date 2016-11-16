package edu.umass.cs.expcode;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNSReply;
import edu.umass.cs.msocket.geocast.CSNodeConfig;
import edu.umass.cs.msocket.geocast.ContextServiceCallsSingleton;
import edu.umass.cs.msocket.geocast.ContextServiceDemultiplexer;
import edu.umass.cs.msocket.geocast.Utils;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;


public class ChurnExpUpdateClass<Integer> implements InterfacePacketDemultiplexer<JSONObject>
{
	public static int NUMGUIDs														= 100;
	
	// decides whether to send query or update
	//private static Random queryUpdateRand;
	
	private Random generalRand;
	
	// used for query workload.
	//private Random queryRand;
	
	public static final String CLIENT_GUID_PREFIX									= "clientGUID";
	
	//private static final HashMap<String, Double> attrValueMap						= new HashMap<String, Double>();
	
	// stores the current values
	private final Integer myID;
	private final CSNodeConfig<Integer> csNodeConfig;
	private final JSONNIOTransport<Integer> niot;
	private final JSONMessenger<Integer> messenger;
	private final String sourceIP;
	private final int sourcePort;
	
	private long updateReqNum														= 0;
	
	private final Object updateReqNumMonitor										= new Object();
	
	
	private LinkedList<InetSocketAddress> nodeList									= null;
	//private final MSocketGroupMember[] mMembers = new MSocketGroupMember[NUMGUIDs];
	
	public ExecutorService	 eservice												= null;
	
	private static int NUMATTRs;
	//private static int requestsps;
	//private static double queryUpdateRatio;
	
	public static final String CONTEXT_ATTR_PREFIX									= "context";
	public static final String REPLY_ADDR_KEY										= "ReplyAddress";
	
	public static String writerName;
	
	private String configFileName													= "contextServiceNodeSetup.txt";
	
	
	public static void main(String [] args) throws Exception
	{
		Integer clientID = Integer.parseInt(args[0]);
		writerName = "writer"+clientID;
		NUMATTRs = Integer.parseInt(args[1]);
		
		NUMGUIDs = Integer.parseInt(args[2]);
		
		ChurnExpUpdateClass<Integer> basicObj 
									= new ChurnExpUpdateClass<Integer>(clientID);
		
		
		for(int i=0; i<NUMGUIDs; i++)
		{
			basicObj.executeUpdateForGUID(i);
			
			/*try
			{
				Thread.sleep(20);
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}*/
		}
		
		// wait for 100 secs
		Thread.sleep(400000);
		
		try
		{
			basicObj.finish();
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		
		System.exit(0);
	}
	
	private long getInterTimePoisson()
	{
		double rate = requestsps;
		
		double interTime = -Math.log(1-poissonRandom.nextDouble()) / rate;
		long returnVal = (long) (interTime*1000);
		return returnVal;
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
	
	private void finish() throws InterruptedException
	{
		stopThis();
		//DefaultGNSClient.gnsClient.stop();
		ContextServiceCallsSingleton.stopThis();
	}
	
	public ChurnExpUpdateClass(Integer id) throws Exception
	{
		nodeList = new LinkedList<InetSocketAddress>();
		
		this.readNodeInfo();
		
		myID = id;
		
		generalRand = new Random(System.currentTimeMillis());
		
		sourcePort = 2000+generalRand.nextInt(50000);
		
		csNodeConfig = new CSNodeConfig<Integer>();
		
		sourceIP =  Utils.getActiveInterfaceInetAddresses().get(0).getHostAddress();
		
		ContextServiceLogger.getLogger().fine("Source IP address "+sourceIP);
		
		csNodeConfig.add(myID, new InetSocketAddress(sourceIP, sourcePort));
        
        AbstractJSONPacketDemultiplexer pd = new ContextServiceDemultiplexer();
		
		ContextServiceLogger.getLogger().fine("\n\n node IP "+csNodeConfig.getNodeAddress(this.myID) +
				" node Port "+csNodeConfig.getNodePort(this.myID)+" nodeID "+this.myID);
		
		niot = new JSONNIOTransport<Integer>(this.myID,  csNodeConfig, pd , true);
		
		messenger = 
			new JSONMessenger<Integer>(niot);
		
		pd.register(ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY, this);
		messenger.addPacketDemultiplexer(pd);
		
		eservice = Executors.newFixedThreadPool(300);
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
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	private void readNodeInfo() throws NumberFormatException, UnknownHostException, IOException
	{
		FileReader fread = new FileReader(configFileName);
		BufferedReader reader = new BufferedReader(fread);
		String line = null;
		// add a leading slash to indicate 'search from the root of the class-path'
		//URL configURL = this.getClass().getResource("/" + configFileName);
		//InputStream stream = configURL.openStream();
		
		//BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
		//String line = null;
		
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
		Random rand = new Random();
		int size = nodeList.size();
		return nodeList.get( rand.nextInt(size) );
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
				String myGUID = getSHA1(realAlias);
				
				Random rand = new Random();
				
				//just for one attribute to enter into context zone
				for(int i=0; i<1; i++)
				//for(int i=0; i<NUMATTRs; i++)
				{
					String oldVal = Double.MIN_VALUE+"";
					String newVal = 1+rand.nextInt(1498) +"";
					long reqNum = 0;
					
					synchronized(updateReqNumMonitor)
					{
						reqNum = updateReqNum++;
					}
					sendUpdateToContextService(myGUID, reqNum, "contextATT"+i ,oldVal, newVal);
				}
			} catch (Exception e1)
			{
				e1.printStackTrace();
			}
		}
		
		public void sendUpdateToContextService(String GUID, long versionNum, String attrName, String oldVal, String newVal)
		{
			try
			{
				ValueUpdateFromGNS<Integer> valUpdFromGNS = 
						new ValueUpdateFromGNS<Integer>(myID, versionNum, GUID, attrName, oldVal, newVal, 
								new JSONObject(), sourceIP, sourcePort, System.currentTimeMillis());
				
				niot.sendToAddress(getRandomNodeSock(), valUpdFromGNS.toJSONObject());
				
				// also send a copy to the monitor
				//niot.sendToAddress(new InetSocketAddress("compute-0-23", 5000), valUpdFromGNS.toJSONObject());
				ContextServiceLogger.getLogger().fine("Value update recvd updID "+valUpdFromGNS.getVersionNum()+" time "+System.currentTimeMillis());
				
			} catch (JSONException e)
			{
				e.printStackTrace();
			} catch (UnknownHostException e)
			{
				e.printStackTrace();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}

	@Override
	public boolean handleMessage(JSONObject jsonObject) 
	{
		try
		{
			if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
					== ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY.getInt() )
			{
				handleUpdateReply(jsonObject);
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return true;
	}
}