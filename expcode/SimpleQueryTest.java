package edu.umass.cs.expcode;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.umass.cs.msocket.geocast.MSocketGroupWriter;


public class SimpleQueryTest
{
	//public static final String configFileName								= "/home/ayadav/contextServiceNodeSetup.txt";
	
	private static Random rand;
	
	public static int QUERY_RATE											= 5000;
	
	public static int NUM_ATTRs											= 100;
	
	public static String writerName;
	
	public ExecutorService	 eservice										= null;
	
	
	public SimpleQueryTest() throws NumberFormatException, UnknownHostException, IOException
	{
		//readNodeInfo();
		eservice = Executors.newFixedThreadPool(1000);
		//newCachedThreadPool();
	}
	
	public static void main(String[] args) throws IOException
	{
		int clientID = Integer.parseInt(args[0]);
		writerName = "writer"+clientID;
		QUERY_RATE = Integer.parseInt(args[1]);
		NUM_ATTRs  = Integer.parseInt(args[2]);
		rand = new Random(clientID);
		
		//int numReqs = Integer.parseInt(args[3]);
		
		SimpleQueryTest basicObj 
											= new SimpleQueryTest();
		//int numReqSent =0;
		//while( numReqSent < numReqs )
		while( true )
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
		    	//numReqSent++;
			}
		}
		
		/*try
		{
			Thread.sleep(10000);
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		basicObj.eservice.shutdownNow();
		DefaultGNSClient.gnsClient.stop();
		ContextServiceCallsSingleton.stopThis();*/
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
				
				testWrit.close();
			} catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
}