package edu.umass.cs.weathercasestudy;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;

public class SearchAndUpdateDriver
{
	public static final double UPD_LOSS_TOLERANCE       		= 0.5;
	public static final double SEARCH_LOSS_TOLERANCE       		= 0.5;
	
	public static final double MIN_UNIX_TIME					= 1385770103;
	public static final double MAX_UNIX_TIME					= 1391127928;
	
	public static final String nomadLogDataPath 				= "/users/ayadavum/nomadLog/loc_seq";
	
	
	public static String csHost									= null;
	public static int csPort									= -1;
	public static int NUMUSERS									= -1;
	public static int myID										= -1;
	public static boolean runSearch								= false;
	
	public static final String latitudeAttr						= "latitude";
	public static final String longitudeAttr					= "longitude";
	
	public static final double minBuffaloLat 					= 42.0;
	public static final double maxBuffaloLat 					= 44.0;
	
	public static final double minBuffaloLong					= -80.0;
	public static final double maxBuffaloLong 					= -78.0;
	
	// in ms
	public static final double TIME_CONTRACTION_EXP_TIME		= 1000.0; // unit is ms
	// for 1 sec
	public static final double TIME_CONTRACTION_REAL_TIME 		= 17859.416666667;  // unit is s
	
	public static  String guidPrefix							= "GUID";
	

	public static int threadPoolSize							= 1;
	
	public static List<GuidEntry> listOfGuidEntries				= null;
	public static final Object guidInsertLock					= new Object();
	
	public static Queue<GNSClient> gnsClientQueue				= new LinkedList<GNSClient>();
	public static final Object queueLock						= new Object();
	
	public static boolean runGNS								= false;
	
	
	public static ExecutorService taskES;
	
	
	public static void main( String[] args ) 
									throws Exception
	{
		csHost = args[0];
		csPort = Integer.parseInt(args[1]);
		NUMUSERS = Integer.parseInt(args[2]);
		myID = Integer.parseInt(args[3]);
		runSearch = Boolean.parseBoolean(args[4]);
		runGNS = Boolean.parseBoolean(args[5]);
		
		guidPrefix = guidPrefix +myID;
		
		if( runGNS )
		{
			threadPoolSize = Integer.parseInt(args[6]);
			
			taskES 			  = Executors.newFixedThreadPool(threadPoolSize);
			
			for( int i=0; i<threadPoolSize; i++ )
			{
				GNSClient gnsClient = new GNSClient();
				gnsClientQueue.add(gnsClient);
			}
			System.out.println("[Client connected to GNS]\n");
			// per 1 ms
			//locationReqsPs = numUsers/granularityOfGeolocationUpdate;
			//userInfoHashMap = new HashMap<String, UserRecordInfo>();
			//taskES = Executors.newCachedThreadPool();
			
			listOfGuidEntries = new LinkedList<GuidEntry>();
			
			long start = System.currentTimeMillis();
			new UserInitializationGNSClass().initializaRateControlledRequestSender();
			long end = System.currentTimeMillis();
			
			System.out.println(NUMUSERS+" initialization complete "+(end-start));
			
			
			IssueUpdatesGNS issUpdGNS = new IssueUpdatesGNS();
			issUpdGNS.readNomadLag();
			issUpdGNS.createTransformedTrajectories();
			
//			System.out.println("minLatData "+issUpd.minLatData+" maxLatData "+issUpd.maxLatData
//					+" minLongData "+issUpd.minLongData+" maxLongData "+issUpd.maxLongData);
			issUpdGNS.printLogStats();
			System.out.println("\n\n");
			issUpdGNS.printRealUserStats();
			
			
			IssueSearchesGNS issueSearchGNS = null;
			if( runSearch )
			{
				issueSearchGNS = new IssueSearchesGNS();
			}
			
			
			Thread th1 = new Thread(new MobilityThreadGNS(issUpdGNS));
			th1.start();
			Thread th2 = null;
			if( runSearch )
			{
				th2 = new Thread(new WeatherThreadGNS(issueSearchGNS));
				th2.start();
			}
			
			try 
			{
				th1.join();
			} catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
			
			if( runSearch )
			{
				try
				{
					th2.join();
				}
				catch ( InterruptedException e )
				{
					e.printStackTrace();
				}
			}
		}
		else
		{
			IssueUpdates issUpd = new IssueUpdates(csHost, csPort, NUMUSERS, myID);
			issUpd.readNomadLag();
			issUpd.createTransformedTrajectories();
			
//			System.out.println("minLatData "+issUpd.minLatData+" maxLatData "+issUpd.maxLatData
//					+" minLongData "+issUpd.minLongData+" maxLongData "+issUpd.maxLongData);
			issUpd.printLogStats();
			System.out.println("\n\n");
			issUpd.printRealUserStats();
			
			
			IssueSearches issueSearch = null;
			if( runSearch )
			{
				issueSearch = new IssueSearches(csHost, csPort);
			}
			
			
			Thread th1 = new Thread(new MobilityThreadCS(issUpd));
			th1.start();
			Thread th2 = null;
			if( runSearch )
			{
				th2 = new Thread(new WeatherThreadCS(issueSearch));
				th2.start();
			}
			
			try 
			{
				th1.join();
			} catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
			
			if( runSearch )
			{
				try
				{
					th2.join();
				}
				catch ( InterruptedException e )
				{
					e.printStackTrace();
				}
			}
		}
		
		
		System.exit(0);
	}
	
	public static GNSClient getGNSClient()
	{
//		int index = rand.nextInt(gnsClientQueue.size());
//		return gnsClientQueue.get(index);
		synchronized(queueLock)
		{
			while(gnsClientQueue.size() == 0)
			{
				try 
				{
					queueLock.wait();
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
			return gnsClientQueue.poll();
		}
	}
	
	public static void returnGNSClient(GNSClient gnsClient)
	{
		synchronized(queueLock)
		{
			gnsClientQueue.add(gnsClient);
			queueLock.notify();
		}
	}
	
	public static class MobilityThreadCS implements Runnable
	{
		private IssueUpdates issUpd;
		
		public MobilityThreadCS(IssueUpdates issUpd)
		{
			this.issUpd = issUpd;
		}
		
		@Override
		public void run()
		{
			try {
				issUpd.runUpdates();
			} catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	public static class WeatherThreadCS implements Runnable
	{
		private IssueSearches issueSearch;
		
		public WeatherThreadCS(IssueSearches issueSearch)
		{
			this.issueSearch = issueSearch;
		}
		
		@Override
		public void run()
		{
			try 
			{
				issueSearch.runSearches();
			}
			catch ( InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	
	public static class MobilityThreadGNS implements Runnable
	{
		private IssueUpdatesGNS issUpdGNS;
		
		public MobilityThreadGNS(IssueUpdatesGNS issUpdGNS)
		{
			this.issUpdGNS = issUpdGNS;
		}
		
		@Override
		public void run()
		{
			try 
			{
				issUpdGNS.runUpdates();
			} catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	public static class WeatherThreadGNS implements Runnable
	{
		private IssueSearchesGNS issueSearchGNS;
		
		public WeatherThreadGNS(IssueSearchesGNS issueSearchGNS)
		{
			this.issueSearchGNS = issueSearchGNS;
		}
		
		@Override
		public void run()
		{
			try 
			{
				issueSearchGNS.runSearches();
			}
			catch ( InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}
	
}