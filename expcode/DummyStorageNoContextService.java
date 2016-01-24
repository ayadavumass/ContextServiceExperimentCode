package edu.umass.cs.expcode;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hyperdex.client.Client;

public class DummyStorageNoContextService 
{
	public static int NUMGUIDs														= 100;
	
	// used for query workload.
	//private Random queryRand;
	
	public static final String CLIENT_GUID_PREFIX									= "clientGUID";
	
	//private final MSocketGroupMember[] mMembers = new MSocketGroupMember[NUMGUIDs];
	
	public ExecutorService	 eservice												= null;
	
	private static int NUMATTRs;
	//private static int requestsps;
	//private static double queryUpdateRatio;
	
	public static final String CONTEXT_ATTR_PREFIX									= "context";
	public static final String REPLY_ADDR_KEY										= "ReplyAddress";
	
	public static String writerName;
	
	public static int clientID;
	
	
	// hyperdex parameters
	// all hyperdex related constants
	public static final String HYPERDEX_IP_ADDRESS 									= "compute-0-23";
	public static final int HYPERDEX_PORT				 							= 4999;
	public static final String HYPERDEX_SPACE										= "contextnet";
	// guid is the key in hyperdex
	public static final String HYPERDEX_KEY_NAME									= "GUID";
			
	public static final int NUM_PARALLEL_CLIENTS									= 50;
			
	private final Client[] hyperdexClientArray										= new Client[NUM_PARALLEL_CLIENTS];
			
	private final ConcurrentLinkedQueue<Client> freeHClientQueue;
			
	private final Object hclientFreeMonitor											= new Object();
	
	
	public static void main(String [] args) throws Exception
	{
		clientID = Integer.parseInt(args[0]);
		writerName = "writer"+clientID;
		NUMATTRs = Integer.parseInt(args[1]);
		
		NUMGUIDs = Integer.parseInt(args[2]);
		
		DummyStorageNoContextService basicObj 
									= new DummyStorageNoContextService();
		
		for(int i=0; i<NUMGUIDs; i++)
		{
			basicObj.executeUpdateForGUID(i);
		}
		
		ContextServiceLogger.getLogger().fine("Data loading complete");
		
		// wait for 100 secs
		Thread.sleep(100000);
		
		try
		{
			basicObj.finish();
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		
		System.exit(0);
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
	}
	
	public DummyStorageNoContextService() throws Exception
	{	
		// hyperdex initialization
		freeHClientQueue = new ConcurrentLinkedQueue<Client>();
					
		for(int i=0;i<NUM_PARALLEL_CLIENTS;i++)
		{
			hyperdexClientArray[i] = new Client(HYPERDEX_IP_ADDRESS, HYPERDEX_PORT);
			
			freeHClientQueue.add(hyperdexClientArray[i]);
		}
		
		eservice = Executors.newFixedThreadPool(100);
	}
	
	private void executeUpdateForGUID( int id )
	{
		eservice.execute( new UpdateGUID(id) );
	}
	
	public void stopThis()
	{
		this.eservice.shutdownNow();
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
				Client HClinetFree = null;
				while( HClinetFree == null )
				{
					HClinetFree = freeHClientQueue.poll();
					
					if( HClinetFree == null )
					{
						synchronized(hclientFreeMonitor)
						{
							try
							{
								hclientFreeMonitor.wait();
							} catch (InterruptedException e)
							{
								e.printStackTrace();
							}
						}
					}
				}
				
				String memberAlias = CLIENT_GUID_PREFIX+clientID;
				String realAlias = memberAlias+id;
				String myGUID = getSHA1(realAlias);
				
				Map<String, Object> attrs = new HashMap<String, Object>();
				
				Random rand = new Random();
				for(int i=0; i<NUMATTRs; i++)
				{
					double newVal = 1+rand.nextInt(1498);
					attrs.put("contextATT"+i, newVal);
				}
				HClinetFree.put( HYPERDEX_SPACE, myGUID, attrs );
				
				
				synchronized(hclientFreeMonitor)
				{
					freeHClientQueue.add(HClinetFree);
					hclientFreeMonitor.notifyAll();
				}
			} catch (Exception e1)
			{
				e1.printStackTrace();
			}
		}
	}
}