package edu.umass.cs.expcode;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.umass.cs.msocket.gns.DefaultGNSClient;


public class GNSClientLibraryTest
{
	public static final String configFileName							= "contextServiceNodeSetup.txt";
	
	public static int QUERY_RATE											= 5000;
	
	public static int NUM_ATTRs											= 100;
	
	public static String writerName;
	
	public ExecutorService	 eservice										= null;
	
	
	public GNSClientLibraryTest() throws NumberFormatException, UnknownHostException, IOException 
	{
		//readNodeInfo();
		eservice = Executors.newCachedThreadPool();
	}	

	
	public static void main(String[] args) throws IOException
	{
		QUERY_RATE = Integer.parseInt(args[0]);
		
		int numReqs = Integer.parseInt(args[1]);
		
		GNSClientLibraryTest basicObj 
											= new GNSClientLibraryTest();
		int numReqSent =0;
		while( numReqSent < numReqs )
		{
			//int startNumAttr = 1;
			//for(int i=0;i<15;i++)
			{
				String query = "guid"+numReqSent;
				
				//basicObj.sendQueryToContextService(query, startNumAttr);
				basicObj.eservice.execute(new SendingQuery(query));
				
		    	try
		    	{
		    		Thread.sleep(30000/QUERY_RATE);
		    	} catch (InterruptedException e)
		    	{
		    		e.printStackTrace();
		    	}
		    	
		    	/*startNumAttr = startNumAttr+2;
		    	
		    	if( startNumAttr > NUM_ATTRs )
		    	{
		    		startNumAttr = NUM_ATTRs;
		    	}*/
		    	numReqSent++;
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
	
	public static class SendingQuery implements Runnable
	{
		private final String query;
		
		public SendingQuery(String query)
		{
			this.query = query;
		}
		
		@Override
		public void run()
		{
			guidCreate(query);
		}
		
		public void guidCreate(String query)
		{
			try
			{
				//ContextServiceLogger.getLogger().fine("Creating query guid "+query);
				long start = System.currentTimeMillis();
				DefaultGNSClient.gnsClient.guidCreate(DefaultGNSClient.myGuidEntry, query);
				long end = System.currentTimeMillis();
				ContextServiceLogger.getLogger().fine("Guid Creation time "+(end-start));
			} catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	
	/*public static String getQueryOfSize(int queryLength)
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
	}*/
}