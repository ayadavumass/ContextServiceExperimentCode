package edu.umass.cs.weathercasestudy;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.messages.RefreshTrigger;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;

public class SearchAndUpdateDriver
{
	public static final double PERIODIC_REFRESH_SLEEP_TIME		= 100.0;  // 10 ms. 1000 ms exp time = 300 mins real time
    // so setting it to 10 ms low value

	public static final double TIME_REQUEST_SLEEP				= 100.0;

	public static final int WAIT_TIME							= 200000;

	public static final int TRIGGER_READING_INTERVAL			= 1000;
	
	public static final double UPD_LOSS_TOLERANCE       		= 0.5;
	public static final double SEARCH_LOSS_TOLERANCE       		= 0.5;
	
	//public static final double MIN_UNIX_TIME					= 1385770103;
	//public static final double MAX_UNIX_TIME					= 1391127928;
	
	
	public static final double EXP_START_TIME					= 1390953600;
	//public static final double EXP_START_TIME					= 1390176000;
	public static final double EXP_END_TIME						= 1391127928;
	
//	public static final double EXP_START_TIME					= 1385770103;
//	public static final double EXP_END_TIME						= 1389644000;
	
	public static final String nomadLogDataPath 				= "/users/ayadavum/nomadLog/loc_seq";
																//= "/home/adipc/Documents/nomadlogData/loc_seq";
	public static final String weatherDataPath 					= "/users/ayadavum/weatherData/weatherEvent.json";
																//= "/home/adipc/Downloads/weatherdata/wwa_201311300000_201401310000/weatherEvent.json";
	public static String csHost									= null;
	public static int csPort									= -1;
	public static int NUMUSERS									= -1;
	public static int myID										= -1;
	
	public static boolean runSearch								= false;
	public static boolean runUpdate								= false;
	
	
	public static final String latitudeAttr						= "latitude";
	public static final String longitudeAttr					= "longitude";
	
	public static final double minBuffaloLat 					= 42.0;
	public static final double maxBuffaloLat 					= 44.0;
	
	public static final double minBuffaloLong					= -80.0;
	public static final double maxBuffaloLong 					= -78.0;
	
	// in ms
	public static final double TIME_CONTRACTION_EXP_TIME		= 1000.0; // unit is ms
	// for 1 sec
	public static final double TIME_CONTRACTION_REAL_TIME 		= 32.282962963;  // unit is s
	
	public static final double TIME_UPDATE_SLEEP_TIME			= 10.0;  // 10 ms. 1000 ms exp time = 300 mins real time
    // so setting it to 10 ms low value
	
	public static  String guidPrefix							= "GUID";
	

	public static int threadPoolSize							= 1;
	
	public static List<GuidEntry> listOfGuidEntries				= null;
	public static final Object guidInsertLock					= new Object();
	
	public static Queue<GNSClient> gnsClientQueue				= new LinkedList<GNSClient>();
	public static final Object queueLock						= new Object();
	
	public static boolean runGNS								= false;
	
	public static int queryRefreshTime;
	
	public static ExecutorService taskES;
	
	public static int numSearchRepetitions;
	
	public static double currentRealTime						= EXP_START_TIME;
	
	public static IssueUpdates2 issUpd;
	public static List<IssueSearches> searchList;
	
	public static boolean triggerEnabled;
	
	public static ContextServiceClient<String> csClient; 
	public static WeatherDataProcessing weatherDataProcess;
	public static void main( String[] args )
									throws Exception
	{
		csHost = args[0];
		csPort = Integer.parseInt(args[1]);
		NUMUSERS = Integer.parseInt(args[2]);
		myID = Integer.parseInt(args[3]);
		runSearch = Boolean.parseBoolean(args[4]);
		runUpdate = Boolean.parseBoolean(args[5]);
		queryRefreshTime = Integer.parseInt(args[6]);
		numSearchRepetitions = Integer.parseInt(args[7]);
		runGNS = Boolean.parseBoolean(args[8]);
		triggerEnabled = Boolean.parseBoolean(args[9]);
		
		guidPrefix = guidPrefix +myID;
		
		if( runGNS )
		{
			threadPoolSize = Integer.parseInt(args[10]);
			
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
			
			TimerThread timer = new TimerThread();
			new Thread(timer).start();
			
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
			issUpd = null;
			//IssueSearches issueSearch = null;
			searchList 				= new LinkedList<IssueSearches>();
			
			if( runUpdate )
			{
				issUpd = new IssueUpdates2(csHost, csPort, NUMUSERS, myID);
				issUpd.readNomadLag();
				issUpd.createTransformedTrajectories();
			
				//			System.out.println("minLatData "+issUpd.minLatData+" maxLatData "+issUpd.maxLatData
				//					+" minLongData "+issUpd.minLongData+" maxLongData "+issUpd.maxLongData);
				issUpd.printLogStats();
				System.out.println("\n\n");
				issUpd.printRealUserStats();
			}
			
			
			if( runSearch )
			{
				csClient = new ContextServiceClient<String>
							(csHost, csPort, ContextServiceClient.HYPERSPACE_BASED_CS_TRANSFORM);
				
				weatherDataProcess = new WeatherDataProcessing();
				for( int i=0; i<numSearchRepetitions; i++ )
				{
					IssueSearches issueSearch 
							= new IssueSearches(csClient, i, weatherDataProcess);
					
					searchList.add(issueSearch);
				}
			}
			
			
			TimerThread timer = new TimerThread();
			new Thread(timer).start();
			
			Thread th1 = null;
			
			//List<Thread> searchThreads = new LinkedList<Thread>();
			
			if( runUpdate )
			{
				th1 = new Thread(new MobilityThreadCS(issUpd));
				th1.start();
			}
			
			IssueSearchRequests searchReqs = null;
			Thread th2 = null;
			if( runSearch )
			{
				searchReqs = new IssueSearchRequests();
				
				th2 = new Thread(searchReqs);
				th2.start();
				
				
				PeriodicRefreshThread periodicRefresh = new PeriodicRefreshThread();
				new Thread(periodicRefresh).start();
				
				
//				for( int i=0; i<searchList.size(); i++ )
//				{
//					Thread th2 = new Thread(new WeatherThreadCS(searchList.get(i)));
//					th2.start();
//					searchThreads.add(th2);
//				}
			}
			
			
			if( runUpdate)
			{
				try 
				{
					th1.join();
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
			
			if( runSearch )
			{
				th2.join();
				
				for( int i=0; i<searchList.size(); i++ )
				{
					IssueSearches appObj = searchList.get(i);
					appObj.setSendingEndTime();
				}
				
				for( int i=0; i<searchList.size(); i++ )
				{
					IssueSearches appObj = searchList.get(i);
					appObj.waitForAppToFinish();
				}
				
//				for( int i=0; i<searchThreads.size(); i++ )
//				{
//					Thread th2 = searchThreads.get(i);
//					try
//					{
//						th2.join();
//					}
//					catch ( InterruptedException e )
//					{
//						e.printStackTrace();
//					}
//				}
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
		private IssueUpdates2 issUpd;
		
		public MobilityThreadCS(IssueUpdates2 issUpd)
		{
			this.issUpd = issUpd;
		}
		
		@Override
		public void run()
		{
			issUpd.runUpdates();
		}
	}
	
//	public static class WeatherThreadCS implements Runnable
//	{
//		private IssueSearches issueSearch;
//		
//		public WeatherThreadCS(IssueSearches issueSearch)
//		{
//			this.issueSearch = issueSearch;
//		}
//		
//		@Override
//		public void run()
//		{
//			try 
//			{
//				issueSearch.runSearches();
//			}
//			catch ( InterruptedException e)
//			{
//				e.printStackTrace();
//			}
//		}
//	}
	
	
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
	
	
	public static class TimerThread implements Runnable
	{
		private final double timeContractFactor;
		private long time = 0;
		public TimerThread()
		{
			timeContractFactor 
					= (TIME_CONTRACTION_REAL_TIME*TIME_UPDATE_SLEEP_TIME)/TIME_CONTRACTION_EXP_TIME;
		}
		
		@Override
		public void run()
		{
			while(true)
			{
				try
				{
					Thread.sleep((long) TIME_UPDATE_SLEEP_TIME);
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
				
				currentRealTime = currentRealTime + timeContractFactor;
				time = time + (long)TIME_UPDATE_SLEEP_TIME;
				if((time % 5000) == 0)
				{
					Date date = new Date((long)currentRealTime*1000L); 
					// *1000 is to convert seconds to milliseconds
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); 
					// the format of your date
					sdf.setTimeZone(TimeZone.getTimeZone("GMT")); 
					// give a timezone reference for formating (see comment at the bottom
					String dateFormat = sdf.format(date);
					
					String printStr = "Date "+dateFormat+" Update ";
					if(issUpd != null)
					{
						printStr = printStr + " "+issUpd.getStatString();
					}
					
					printStr = printStr+" "+ " Search ";
					for(int i=0; i<searchList.size(); i++)
					{
						printStr = printStr +searchList.get(i).getStatString()+" ";
					}
					
					System.out.println(printStr);
				}
				
			}
		}
	}
	
	
	private static class IssueSearchRequests implements Runnable
	{
		
		public IssueSearchRequests()
		{
			for(int i=0; i<searchList.size(); i++)
			{
				IssueSearches appObj = searchList.get(i);
				appObj.startExpTime();
			}
		}
		
		@Override
		public void run() 
		{
			while( SearchAndUpdateDriver.currentRealTime <= SearchAndUpdateDriver.EXP_END_TIME )
			{
				try
				{
					Thread.sleep((long) TIME_REQUEST_SLEEP);
				}
				catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
				
				
				for(int i=0; i<searchList.size(); i++)
				{
					IssueSearches appObj = searchList.get(i);
					appObj.sendSearchesWhoseTimeHasCome();
				}
			}
		}
	}
	
	private static class PeriodicRefreshThread implements Runnable
	{
		@Override
		public void run()
		{
			while( true )
			{
				try
				{
					Thread.sleep((long) PERIODIC_REFRESH_SLEEP_TIME);
				}
				catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
				
				for(int i=0; i<searchList.size(); i++)
				{
					IssueSearches appObj = searchList.get(i);
					appObj.performRefreshQueries();
				}
			}
		}
	}
	
	
	public static class ReadTriggerRecvd implements Runnable
	{
		@Override
		public void run()
		{
			while(true)
			{
				JSONArray triggerArray = new JSONArray();
				csClient.getQueryUpdateTriggers(triggerArray);
				
//				System.out.println("Reading triggers num read "
//												+triggerArray.length());
				
				for( int i=0;i<triggerArray.length();i++ )
				{
					try 
					{
						RefreshTrigger<Integer> refreshTrig 
							= new RefreshTrigger<Integer>(triggerArray.getJSONObject(i));
						long timeTakenSinceUpdate 
							= System.currentTimeMillis() - refreshTrig.getUpdateStartTime();
						if(timeTakenSinceUpdate <= 0)
						{
							System.out.println("Trigger recvd time sync issue between two machines ");
						}
						else
						{
							System.out.println("Trigger recvd time taken "+timeTakenSinceUpdate);
						}
					} catch (JSONException e) 
					{
						e.printStackTrace();
					}
				}
				
				try
				{
					Thread.sleep(SearchAndUpdateDriver.TRIGGER_READING_INTERVAL);
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
	}
	
}