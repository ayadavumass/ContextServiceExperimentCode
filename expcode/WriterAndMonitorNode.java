package edu.umass.cs.expcode;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.RefreshTrigger;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.msocket.geocast.CSNodeConfig;
import edu.umass.cs.msocket.geocast.ContextServiceDemultiplexer;
import edu.umass.cs.msocket.geocast.Utils;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;


public class WriterAndMonitorNode<Integer> implements InterfacePacketDemultiplexer<JSONObject>
{	
	public static final int MAX_QUERY_LEN = 10;
	
	// 100 seconds, experiment runs for 100 seconds
	public static final int EXPERIMENT_TIME										= 100000;
	
	// after sending all the requests it waits for 100 seconds 
	public static final int WAIT_TIME											= 300000;
	
	public static final int TOTAL_NUM_REQS										= 8000;
	
	private String configFileName												= "contextServiceNodeSetup.txt";
	
	
	public static final String CLIENT_GUID_PREFIX								= "clientGUID";
	
	//private static final HashMap<String, Double> attrValueMap					= new HashMap<String, Double>();
	
	// stores the current values
	private final Integer myID;
	private final CSNodeConfig<Integer> csNodeConfig;
	private final JSONNIOTransport<Integer> niot;
	private final JSONMessenger<Integer> messenger;
	private final String sourceIP;
	private final int sourcePort;
	
	public ExecutorService	 eservice											= null;
	
	// used in query work load, to keep track of current
	// number of attributes in the query
	private int currNumAttr;
	
	
	private LinkedList<InetSocketAddress> nodeList								= null;
	
	private Random rand															= null;
	
	
	public static String writerName;
	
	
	public static void main(String [] args) throws Exception
	{
		
		Integer clientID = Integer.parseInt(args[0]);
		writerName = "writer"+clientID;
		
		
		
		WriterAndMonitorNode<Integer> basicObj 
									= new WriterAndMonitorNode<Integer>(clientID);
		
		// send a query and then wait for the refresh triggers and update triggers.
		//basicObj.sendQuery(0);
		
		//basicObj.waitForFinish();
		
		// wait for 100 secs
		Thread.sleep(400000);
				
		// just exit JVM
		System.exit(0);
	}
	
	public WriterAndMonitorNode(Integer id) throws Exception
	{
		
		rand = new Random(System.currentTimeMillis());
		
		nodeList = new LinkedList<InetSocketAddress>();
		
		readNodeInfo();
		myID = id;
		
		currNumAttr = 1;
		
		//String memberAlias = CLIENT_GUID_PREFIX+id;
		
		sourcePort = 5000;
		//START_PORT+Integer.parseInt(myID.toString());
		
		csNodeConfig = new CSNodeConfig<Integer>();
		
		sourceIP =  Utils.getActiveInterfaceInetAddresses().get(0).getHostAddress();
		
		ContextServiceLogger.getLogger().fine("Source IP address "+sourceIP);
		
		csNodeConfig.add(myID, new InetSocketAddress(sourceIP, sourcePort));
        
        AbstractJSONPacketDemultiplexer pd = new ContextServiceDemultiplexer();
		
		ContextServiceLogger.getLogger().fine("\n\n node IP "+csNodeConfig.getNodeAddress(this.myID) +
				" node Port "+csNodeConfig.getNodePort(this.myID)+" nodeID "+this.myID+" nodeList "+nodeList.size());
		
		niot = new JSONNIOTransport<Integer>(this.myID,  csNodeConfig, pd , true);
		
		messenger = 
			new JSONMessenger<Integer>(niot);
		
		//pd.register(ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY, this);
		pd.register(ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY, this);
		pd.register(ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS, this);
		pd.register(ContextServicePacket.PacketType.REFRESH_TRIGGER, this);
		
		messenger.addPacketDemultiplexer(pd);
		
		eservice = Executors.newFixedThreadPool(100);
	}
	
	private void handleUpdate(JSONObject jsonObject)
	{
		try
		{
			ValueUpdateFromGNS<Integer> valupdate = new ValueUpdateFromGNS<Integer>(jsonObject);
			ContextServiceLogger.getLogger().fine("Value update recvd updID "+valupdate.getVersionNum()+" time "+System.currentTimeMillis());
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	private void handleRefreshTrigger(JSONObject jsonObject)
	{
		try 
		{
			RefreshTrigger<Integer> refTrig = new RefreshTrigger<Integer>(jsonObject);
			ContextServiceLogger.getLogger().fine("Refresh trigger recvd updID "+refTrig.getVersionNum()+" time "+System.currentTimeMillis());
		} catch (JSONException e) 
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean handleMessage(JSONObject jsonObject) 
	{
		try
		{
			//ContextServiceLogger.getLogger().fine("handleJSONObject called. "+jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
			//		+" "+ jsonObject);
			if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE) 
					== ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY.getInt() )
			{
				ContextServiceLogger.getLogger().fine("Query mesg reply recvd");
				//handleQueryReply(jsonObject);
			} else if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
					== ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS.getInt() )
			{
				handleUpdate(jsonObject);
			} else if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
					== ContextServicePacket.PacketType.REFRESH_TRIGGER.getInt() )
			{
				handleRefreshTrigger(jsonObject);
			}
			/*else if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE) 
					== ContextServicePacket.PacketType.REFRESH_TRIGGER.getInt() )
			{
				handleRefreshTrigger(jsonObject);
			}*/
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return true;
	}
	
	/**
	 * Depending on the random outcome this function sends query
	 */
	public void sendQuery(long currID)
	{
		//String query = getQueryOfSize(currNumAttr);
		//general query
		String query = "1 <= contextATT0 <= 1500";
		currNumAttr = 1;
		eservice.execute(new SendingRequest(currID, SendingRequest.QUERY, query, currNumAttr, "", -1, -1) );
		//currNumAttr = currNumAttr + 2;
	}
	
	public void stopThis()
	{
		this.eservice.shutdownNow();
		this.niot.stop();
		this.messenger.stop();
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
		return nodeList.get( rand.nextInt(size) );
	}
	
	public class SendingRequest implements Runnable
	{
		public static final int QUERY  = 2;
		
		private final int reqType;
		private final String query;
		private final int numAttr;
		private final String attrName;
		private final double nextVal;
		private final long currID;
		private final double oldVal;
		
		public SendingRequest(long currID, int reqType, String query, int numAttr, 
				String attrName, double nextVal, double oldVal)
		{
			this.reqType = reqType;
			this.query = query;
			this.numAttr = numAttr;
			this.attrName = attrName;
			this.nextVal = nextVal;
			this.currID = currID;
			this.oldVal = oldVal;
		}
		
		@Override
		public void run()
		{
			switch(reqType)
			{
				case QUERY:
				{
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
			}
		}
		
		private void sendQueryToContextService(String query, long userReqNum) throws IOException, JSONException
		{
			QueryMsgFromUser<Integer> qmesgU 
				= new QueryMsgFromUser<Integer>(myID, query, sourceIP, sourcePort, userReqNum);
			
			InetSocketAddress sockAddr = getRandomNodeSock();
			//ContextServiceLogger.getLogger().fine("Sending query to "+sockAddr);
			niot.sendToAddress(sockAddr, qmesgU.toJSONObject());
		}
	}
}