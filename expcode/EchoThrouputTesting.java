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
import edu.umass.cs.contextservice.messages.EchoMessage;
import edu.umass.cs.msocket.geocast.CSNodeConfig;
import edu.umass.cs.msocket.geocast.ContextServiceCallsSingleton;
import edu.umass.cs.msocket.geocast.ContextServiceDemultiplexer;
import edu.umass.cs.msocket.geocast.Utils;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;


public class EchoThrouputTesting<NodeIDType> implements InterfacePacketDemultiplexer<JSONObject>
{
	//public static final String DUMMYGUID = "0B3C3AC6E25FF553BE3DC9176889E927C14CEA2A";
		public static final int MAX_QUERY_LEN = 10;
		
		// 100 seconds, experiment runs for 100 seconds
		public static final int EXPERIMENT_TIME										= 50000;
		
		// after sending all the requests it waits for 100 seconds 
		public static final int WAIT_TIME											= 200000;
		
		//public static final int TOTAL_NUM_REQS									= 8000;
		
		private String configFileName												= "contextServiceNodeSetup.txt";
		
		private Random generalRand;
		
		public static final String CLIENT_GUID_PREFIX								= "clientGUID";
		
		//private static final HashMap<String, Double> attrValueMap					= new HashMap<String, Double>();
		
		// stores the current values
		private final NodeIDType myID;
		private final CSNodeConfig<NodeIDType> csNodeConfig;
		private final JSONNIOTransport<NodeIDType> niot;
		private final JSONMessenger<NodeIDType> messenger;
		private final String sourceIP;
		private final int sourcePort;
		
		public ExecutorService	 eservice											= null;
		
		private int numberRepliesRecvd												= 0;
		
		private static double currNumReqSent 										= 0;
		
		
		private final Object repliesRecvMonitor										= new Object();
		//private final Object queryReplyRecvMonitor								= new Object();
		
		private final Object finishMonitor											= new Object();
		
		
		private LinkedList<InetSocketAddress> nodeList								= null;
		
		private Random rand															= null;
		
		//private JSONObject	attrValueObject										= null;
		
		private static double requestsps;
		
		
		public static void main(String [] args) throws Exception
		{	
			Integer clientID = Integer.parseInt(args[0]);
			requestsps = Double.parseDouble(args[1]);
			
			EchoThrouputTesting<Integer> basicObj 
										= new EchoThrouputTesting<Integer>(clientID);
			
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
			
			basicObj.waitForFinish();
			double endTimeReplyRecvd = System.currentTimeMillis();
			double sysThrput= (currNumReqSent * 1000.0)/(endTimeReplyRecvd - startTime);
			
			ContextServiceLogger.getLogger().fine("Result: Eventual sending rate "+sendingRate+" Throughput "+sysThrput);
			
			System.exit(0);
		}
		
		private static void sendAMessage(EchoThrouputTesting<Integer> basicObj)
		{	
			basicObj.sendEcho();	
		}
		
		private void waitForFinish()
		{	
			while( this.numberRepliesRecvd < currNumReqSent )
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
			finish();
		}
		
		private void finish()
		{
			try
			{
				stopThis();
				ContextServiceCallsSingleton.stopThis();
				//if(DefaultGNSClient.gnsClient != null)
				//{
				//	DefaultGNSClient.gnsClient.stop();
				//}
			} catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
		
		public EchoThrouputTesting(NodeIDType id) throws Exception
		{
			generalRand = new Random();
			rand = new Random();
			
			nodeList = new LinkedList<InetSocketAddress>();
			
			readNodeInfo();
			myID = id;
			
			sourcePort = 2000+generalRand.nextInt(50000);
			
			csNodeConfig = new CSNodeConfig<NodeIDType>();
			
			sourceIP =  Utils.getActiveInterfaceInetAddresses().get(0).getHostAddress();
			
			ContextServiceLogger.getLogger().fine("Source IP address "+sourceIP);
			
			csNodeConfig.add(myID, new InetSocketAddress(sourceIP, sourcePort));
	        
	        AbstractJSONPacketDemultiplexer pd = new ContextServiceDemultiplexer();
			
			ContextServiceLogger.getLogger().fine("\n\n node IP "+csNodeConfig.getNodeAddress(this.myID) +
					" node Port "+csNodeConfig.getNodePort(this.myID)+" nodeID "+this.myID);
			
			niot = new JSONNIOTransport<NodeIDType>(this.myID,  csNodeConfig, pd , true);
			
			messenger = 
				new JSONMessenger<NodeIDType>(niot);
			
			pd.register(ContextServicePacket.PacketType.ECHOREPLY_MESSAGE, this);
			
			messenger.addPacketDemultiplexer(pd);
			
			eservice = Executors.newFixedThreadPool(1000);
		}
		
		/**
		 * Depending on the random outcome this function sends query
		 */
		public void sendEcho()
		{
			eservice.execute(new SendingRequest() );
		}
		
		public void stopThis()
		{
			this.eservice.shutdownNow();
			this.niot.stop();
			this.messenger.stop();
		}
		
		private void handleEchoReply(JSONObject jso)
		{
			synchronized(repliesRecvMonitor)
			{
				numberRepliesRecvd++;
				ContextServiceLogger.getLogger().fine("repliesRecvMonitor "+numberRepliesRecvd);
				
				if( numberRepliesRecvd >= currNumReqSent )
				{
					synchronized(finishMonitor)
					{
						finishMonitor.notify();
					}
				}
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
			return nodeList.get( rand.nextInt(size) );
		}
		
		/*public class WaitTimerTask extends TimerTask
		{
			@Override
			public void run()
			{
				// print the remaining update and query times
				// and finish the process, cancel the timer and exit JVM.
				finish();
				
				long now = System.currentTimeMillis();
				
				//Iterator hashIterator = 
				for( long reqID: queryHashMap.keySet() )
				{
					long startTime = queryHashMap.get(reqID);
					ContextServiceLogger.getLogger().fine("TimeOutMSOCKETWRITERINTERNAL from CS querytime "
						+(now-startTime) +" numAttr "+0+" cstime "+(now-startTime));
				}
				queryHashMap.clear();
				
				
				for( long reqID: updateHashMap.keySet() )
				{
					long startTime = updateHashMap.get(reqID);
					
					ContextServiceLogger.getLogger().fine
						("TimeOutUpdate reply recvd tillContextTime"+(now-startTime)+" contextProcessTime "+(now-startTime)+
							" totalTime "+(now-startTime));
					
				}
				updateHashMap.clear();
				
				//while( !updateQueue.isEmpty() )
				//{
				//	Long startTime = updateQueue.poll();
				//	if( startTime != null )
				//	{
				//		ContextServiceLogger.getLogger().fine
				//		("TimeOutUpdate reply recvd tillContextTime"+(now-startTime)+" contextProcessTime "+(now-startTime)+
				//				" totalTime "+(now-startTime));
				//	}
				//}
				
				waitTimer.cancel();
				// just terminate the JVM
				System.exit(0);
			}
		}*/
		
		public class SendingRequest implements Runnable
		{	
			@Override
			public void run()
			{
				try 
				{
					sendEchoToContextService();
				} catch (IOException e) 
				{
					e.printStackTrace();
				} catch (JSONException e) 
				{
					e.printStackTrace();
				}
			}
			
			private void sendEchoToContextService() throws IOException, JSONException
			{
				EchoMessage<NodeIDType> qmesgU 
					= new EchoMessage<NodeIDType>(myID, "echo", sourceIP, sourcePort);
				
				InetSocketAddress sockAddr = getRandomNodeSock();
				//ContextServiceLogger.getLogger().fine("Sending query to "+sockAddr);
				niot.sendToAddress(sockAddr, qmesgU.toJSONObject());
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
						== ContextServicePacket.PacketType.ECHOREPLY_MESSAGE.getInt() )
				{
					handleEchoReply(jsonObject);
				}
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			return true;
		}
}