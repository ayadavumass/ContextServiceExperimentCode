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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNSReply;
import edu.umass.cs.msocket.contextsocket.CSNodeConfig;
import edu.umass.cs.msocket.contextsocket.ContextServiceDemultiplexer;
import edu.umass.cs.msocket.contextsocket.Utils;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
//import edu.umass.cs.msocket.gns.DefaultGNSClient;
import edu.umass.cs.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;


public class DummyStorageNoGNS<Integer> implements InterfacePacketDemultiplexer<JSONObject>
{
	// 1% loss tolerance
	public static final double LOSS_TOLERANCE										= 0.00;
	
	// after sending all the requests it waits for 100 seconds 
	public static final int WAIT_TIME												= 200000;
	
	public static int NUMGUIDs														= 100;
	
	// decides whether to send query or update
	//private static Random queryUpdateRand;
	
	private Random generalRand;
	
	private final Random valueRand;
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
	
	private static long userReqIdNum												= 0;
	
	private long currNumReplyRecvd													= 0;
	
	private final Timer waitTimer;
	
	private final Object replyWaitMonitor											= new Object();
	
	public static void main(String [] args) throws Exception
	{
		Integer clientID = Integer.parseInt(args[0]);
		writerName = "writer"+clientID;
		NUMATTRs = Integer.parseInt(args[1]);
		
		NUMGUIDs = Integer.parseInt(args[2]);
		
		DummyStorageNoGNS<Integer> basicObj 
									= new DummyStorageNoGNS<Integer>(clientID);
		
		for(int i=0; i<NUMGUIDs; i++)
		{
			basicObj.executeUpdateForGUID(i, userReqIdNum++);
			Thread.sleep(15);
		}
		
		// wait for 100 secs
		//Thread.sleep(200000);
		
		basicObj.waitForFinish();
		/*try
		{
			basicObj.finish();
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}*/
		
		System.exit(0);
	}
	
	
	private void waitForFinish()
	{
		waitTimer.schedule(new WaitTimerTask(), WAIT_TIME);
		
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
	
	private boolean checkForCompletionWithLossTolerance()
	{
		boolean completion = false;
		
		double withinLoss = (LOSS_TOLERANCE * NUMGUIDs)/100.0;
		if( (NUMGUIDs - currNumReplyRecvd) <= withinLoss )
		{
			completion = true;
		}
		return completion;
	}
	
	public class WaitTimerTask extends TimerTask
	{
		@Override
		public void run()
		{
			// print the remaining update and query times
			// and finish the process, cancel the timer and exit JVM.
			stopThis();
			//double endTimeReplyRecvd = System.currentTimeMillis();
			//double sysThrput= (currNumReplyRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
			
			//ContextServiceLogger.getLogger().fine("Result:TimeOutThroughput "+sysThrput);
			
			waitTimer.cancel();
			// just terminate the JVM
			System.exit(0);
		}
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
       String returnGUID = sb.toString();
       return returnGUID.substring(0, 40);
	}
	
	/*private void finish() throws InterruptedException
	{
		stopThis();
		//DefaultGNSClient.gnsClient.stop();
		ContextServiceCallsSingleton.stopThis();
	}*/
	
	public DummyStorageNoGNS(Integer id) throws Exception
	{
		nodeList = new LinkedList<InetSocketAddress>();
		
		this.readNodeInfo();
		
		myID = id;
		
		generalRand = new Random(System.currentTimeMillis());
		
		valueRand = new Random((Integer)myID);
		
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
		
		eservice = Executors.newCachedThreadPool();
		//eservice = Executors.newFixedThreadPool(1000);
		waitTimer = new Timer(); 
	}
	
	private void executeUpdateForGUID( int id, long userReqId )
	{
		eservice.execute( new UpdateGUID(id, userReqId) );
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
			long totalTime = currTime ;
			
			synchronized(this.replyWaitMonitor)
			{
				currNumReplyRecvd++;
				if(currNumReplyRecvd == NUMGUIDs)
				{
					this.replyWaitMonitor.notify();
				}
			}
			
			ContextServiceLogger.getLogger().fine
			("Update completion requestID "+vur.getUserReqNum()+" time "+currTime);
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
		int size = nodeList.size();
		return nodeList.get( generalRand.nextInt(size) );
	}
	
	
	public class UpdateGUID implements Runnable
	{
		private final int id;
		private final long userReqId;
		
		public UpdateGUID(int id, long userReqId)
		{
			this.id = id;
			this.userReqId = userReqId;
		}
		
		@Override
		public void run()
		{
			try
			{
				String memberAlias = CLIENT_GUID_PREFIX+myID;
				String realAlias = memberAlias+id;
				String myGUID = DummyStorageNoGNS.getSHA1(realAlias);
				
				JSONObject attrValuePairs = new JSONObject();
				
				for(int i=0; i<NUMATTRs; i++)
				{
					//String oldVal = Double.MIN_VALUE+"";
					double newVal = 1+valueRand.nextInt(1498);
					attrValuePairs.put("contextATT"+i, newVal);
				}
				
				sendUpdateToContextService(myGUID, userReqId, attrValuePairs);
			} catch (Exception e1)
			{
				e1.printStackTrace();
			}
		}
		
		public void sendUpdateToContextService(String GUID, long versionNum, JSONObject attrValuePairs)
		{
			try
			{
				ValueUpdateFromGNS<Integer> valUpdFromGNS = 
						new ValueUpdateFromGNS<Integer>(myID, versionNum, GUID, 
								attrValuePairs, sourceIP, sourcePort, versionNum);
				InetSocketAddress randomSock = getRandomNodeSock();
				
				niot.sendToAddress(randomSock, valUpdFromGNS.toJSONObject());
				ContextServiceLogger.getLogger().fine("Sending update requestID "+versionNum+" time "+System.currentTimeMillis());
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
				//ContextServiceLogger.getLogger().fine("JSON packet recvd "+jsonObject);
				handleUpdateReply(jsonObject);
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return true;
	}
}
