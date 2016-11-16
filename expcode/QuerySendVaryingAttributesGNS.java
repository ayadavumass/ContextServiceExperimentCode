package edu.umass.cs.expcode;


import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import edu.umass.cs.msocket.geocast.MSocketGroupWriter;

public class QuerySendVaryingAttributesGNS 
{
	public static final String configFileName							= "100nodesSetup.txt";
	
	public static final int START_PORT									= 9189;
	
	private static final Random rand = new Random();
	
	//private static final HashMap<Integer, InetSocketAddress> nodeMap			
	//						= new HashMap<Integer, InetSocketAddress>();
	
	// trigger mesg type
	//public static final int QUERY_MSG_FROM_USER 		= 2;
	//private enum Keys {QUERY};
	
	//private final Integer myID;
	//private final CSNodeConfig<Integer> csNodeConfig;
	//private final JSONNIOTransport<Integer> niot;
	//private final String sourceIP;
	//private final int listenPort;
	
	//private int requestCounter												= 0;
	
	public static int QUERY_RATE											= 5000;
	
	public static int NUM_ATTRs											= 100;
	
	public static String writerName;
	
	private ExecutorService	 eservice										= null;
	
	
	public QuerySendVaryingAttributesGNS() throws NumberFormatException, UnknownHostException, IOException 
	{
		//readNodeInfo();
		eservice = Executors.newCachedThreadPool();
		
		//myID = id;
		//listenPort = START_PORT+Integer.parseInt(myID.toString());
		
		//csNodeConfig = new CSNodeConfig<Integer>();
		
		//sourceIP =  Utils.getActiveInterfaceInetAddresses().get(0).getHostAddress();
		
		//ContextServiceLogger.getLogger().fine("Source IP address "+sourceIP);
		
		//csNodeConfig.add(myID, new InetSocketAddress(sourceIP, listenPort));
        
        //AbstractPacketDemultiplexer pd = new ContextServiceDemultiplexer();
		//ContextServicePacketDemultiplexer pd;
		
		//ContextServiceLogger.getLogger().fine("\n\n node IP "+csNodeConfig.getNodeAddress(this.myID)+
	    //			" node Port "+csNodeConfig.getNodePort(this.myID)+" nodeID "+this.myID);
		
		//niot = new JSONNIOTransport<Integer>(this.myID,  csNodeConfig, pd , true);
		
		//JSONMessenger<Integer> messenger = 
		//	new JSONMessenger<Integer>(niot.enableStampSenderInfo());
		
		//pd.register(ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY, this);
		//messenger.addPacketDemultiplexer(pd);
	}
	
	/*public void handleQueryReply(JSONObject jso)
	{
		try
		{
			long time = System.currentTimeMillis();
			QueryMsgFromUserReply<Integer> qmur;
			qmur = new QueryMsgFromUserReply<Integer>(jso);
			ContextServiceLogger.getLogger().fine("CONTEXTSERVICE EXPERIMENT: QUERYFROMUSERREPLY REQUEST ID "
					+qmur.getUserReqNum()+" NUMATTR "+0+" AT "+time+" EndTime "
					+time+ " QUERY ANSWER "+qmur.getResultGUIDs());
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}*/
	
	public static void main(String[] args) throws IOException
	{
		//Integer clientID = Integer.parseInt(args[0]);
		writerName = args[0];
		QUERY_RATE = Integer.parseInt(args[1]);
		NUM_ATTRs  = Integer.parseInt(args[2]);
		
		QuerySendVaryingAttributesGNS basicObj 
											= new QuerySendVaryingAttributesGNS();
		/*while(true)
		{
			String query = getQueryOfSize(NUM_ATTRs);
			basicObj.sendQueryToContextService(query, NUM_ATTRs);
	    	try
	    	{
	    		Thread.sleep(1000/QUERY_RATE);
			} catch (InterruptedException e)
	    	{
	    		e.printStackTrace();
	    	}
		}*/
		
		while(true)
		{
			int startNumAttr = 1;
			for(int i=0;i<15;i++)
			{
				String query = getQueryOfSize(startNumAttr);
				
				//basicObj.sendQueryToContextService(query, startNumAttr);
				basicObj.eservice.execute(new SendingQuery(query, startNumAttr));
				
		    	try
		    	{
		    		Thread.sleep(30000/QUERY_RATE);
				} catch (InterruptedException e)
		    	{
		    		e.printStackTrace();
		    	}
		    	
		    	startNumAttr = startNumAttr+2;
		    	
		    	if( startNumAttr > NUM_ATTRs )
		    	{
		    		startNumAttr = NUM_ATTRs;
		    	}
			}
		}
	}
	
	public static String getQueryOfSize(int queryLength)
	{
		String query="";
	    for(int i=0;i<queryLength;i++)
	    {
	    	int attrNum = rand.nextInt(NUM_ATTRs);
	    	
	    	String predicate = "1 <= contextATT"+attrNum+" <= 5";
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
	
	public static class SendingQuery implements Runnable
	{
		private final String query;
		private final int numAttr;
		
		public SendingQuery(String query, int numAttr)
		{
			this.query = query;
			this.numAttr = numAttr;
		}
		
		@Override
		public void run()
		{
			sendQueryToContextService(query, numAttr);
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
				
				ContextServiceLogger.getLogger().fine("CONTEXTSERVICE EXPERIMENT: QUERYTIME NUMATTR "+numAttr+" TIME "+(endTime-startTime));
				
				/*QueryMsgFromUser<Integer> qmesgU 
					= new QueryMsgFromUser<Integer>(myID, query, sourceIP, listenPort, userReqNum);
				Set<Integer> keySet= nodeMap.keySet();
				int randIndex = rand.nextInt(keySet.size());
				InetSocketAddress toMe = nodeMap.get(keySet.toArray()[randIndex]);	
		        niot.sendToAddress(toMe, qmesgU.toJSONObject());
				ContextServiceLogger.getLogger().fine("Query sent "+query+" to "+toMe.getAddress()+" port "+toMe.getPort());*/
			} catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * sends 10 queries 10 seconds apart, with increasing number of attributes
	 * @param basicObj
	 */
	/*public static void randomizedQueryWorkload(BasicContextQuerySendExp<Integer> basicObj)
	{
		int startNumAttr = 1;
		for(int i=0;i<15;i++)
		{
			String query = getQueryOfSize(startNumAttr);
			basicObj.sendQueryToContextService(query, startNumAttr);
			
	    	try
	    	{
	    		Thread.sleep(3000);
	    	} catch (InterruptedException e)
	    	{
	    		e.printStackTrace();
	    	}
	    	startNumAttr = startNumAttr+2;
		}
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