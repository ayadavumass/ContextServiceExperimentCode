package edu.umass.cs.weathercasestudy;

import java.awt.geom.Path2D;
import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


import edu.umass.cs.acs.geodesy.GlobalCoordinate;
import edu.umass.cs.contextservice.client.ContextServiceClient;

public class IssueSearches extends AbstractSearchRequestSendingClass
{
	private WeatherDataProcessing weatherDataProcess;
	
	public static ContextServiceClient csClient;
	public static boolean useGNS							= false;
	
	
	private int nextIndexToSend 							= 0;
	
	//private long requestId									= 0;
	private double numSearch								= 0;
	private double sumSearchLatency							= 0;
	private double sumResultSize							= 0;
	
	private ConcurrentHashMap<String, ActiveQueryStorage> activeQMap;
	
	//private PeriodicRefreshThread periodicRefreshThread;
	//private ReadTriggerRecvd readTriggers;
	
	//private final int searchId;
	//private long searchStartTime;
	private long searchEndTime;
	
	private double sumLatitude								= 0.0;
	private double sumLongitude								= 0.0;
	private double numOriginalSearch						= 0.0;
	
	private Object numSentLock								= new Object();
	
	public IssueSearches( ContextServiceClient csclient, 
			int searchId, WeatherDataProcessing weatherDataProcess) throws NoSuchAlgorithmException, IOException
	{
		super();
		
		//this.refreshTimeInSec 	 = refreshTimeInSec;
		//this.searchId			 	= searchId;
		//weatherDataProcess 		 = new WeatherDataProcessing();
		this.weatherDataProcess 	= weatherDataProcess;
		csClient  = csclient;
		
		if(!SearchAndUpdateDriver.triggerEnabled)
		{
			activeQMap 				 = new ConcurrentHashMap<String, ActiveQueryStorage>();
			
			//periodicRefreshThread    = new PeriodicRefreshThread();
			
			//new Thread(periodicRefreshThread).start();
		}
		else
		{
			//readTriggers = new ReadTriggerRecvd();
			//new Thread(readTriggers).start();
		}
	}
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
	}
	
	@Override
	public void incrementSearchNumRecvd(int resultSize, long timeTaken)
	{
		synchronized(waitLock)
		{
			numRecvd++;
			numSearch++;
//			System.out.println("Updates recvd "+userGUID+" time "+timeTaken
//					+" numRecvd "+numRecvd+" numSent "+numSent);
			this.sumSearchLatency = this.sumSearchLatency + timeTaken;
			this.sumResultSize = this.sumResultSize + resultSize;
			if(checkForCompletionWithLossTolerance(numSent, numRecvd))
			{
				waitLock.notify();
			}
		}
	}
	
//	public void setSendingStartTime()
//	{
//		searchStartTime = System.currentTimeMillis();
//	}
	
	public void setSendingEndTime()
	{
		searchEndTime = System.currentTimeMillis();
	}
	
	
//	public void runSearches() throws InterruptedException
//	{
//		searchStartTime = System.currentTimeMillis();
//		while( SearchAndUpdateDriver.currentRealTime <= SearchAndUpdateDriver.EXP_END_TIME )
//		{
////			Date date = new Date((long)SearchAndUpdateDriver.currentRealTime*1000L); 
////						// *1000 is to convert seconds to milliseconds
////			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); 
////						// the format of your date
////			sdf.setTimeZone(TimeZone.getTimeZone("GMT")); 
////						// give a timezone reference for formating (see comment at the bottom
////			String dateFormat = sdf.format(date);
////			System.out.println("Search: Current simulated time "
////					+ SearchAndUpdateDriver.currentRealTime+" time in GMT-5 "
////					+ dateFormat+" numSent "+numSent+" numRecvd "+numRecvd+" avg reply size "
////					+ (sumResultSize/numRecvd));
//			
//			sendSearchesWhoseTimeHasCome();
//			Thread.sleep((long)TIME_REQUEST_SLEEP);
//		}
//		long end = System.currentTimeMillis();
//		double sendingRate = (numSent*1000.0)/(end-searchStartTime);
//		System.out.println("Search eventual sending rate "+sendingRate+" reqs/s");
//		this.waitForFinish();
//		long endTime = System.currentTimeMillis();
//		double systemThpt = (numRecvd*1000.0)/(endTime-searchStartTime);
//		System.out.println("Search system throughput "+systemThpt+" reqs/s");
//		System.out.println("Search avg search latency "+(sumSearchLatency/numRecvd)+" ms "
//				+" result size "+(sumResultSize/numRecvd));
//	}
	
	public void waitForAppToFinish()
	{
		double sendingRate = (numSent*1000.0)/(searchEndTime-expStartTime);
		System.out.println("Search eventual sending rate "+sendingRate+" reqs/s");
		this.waitForFinish();
		long endTime = System.currentTimeMillis();
		double systemThpt = (numRecvd*1000.0)/(endTime-expStartTime);
		System.out.println("Search system throughput "+systemThpt+" reqs/s");
		System.out.println("Search avg search latency "+(sumSearchLatency/numRecvd)+" ms "
				+" result size "+(sumResultSize/numRecvd));
	}
	
	public void performRefreshQueries()
	{
//		try
//		{
//			Thread.sleep((long) PERIODIC_REFRESH_SLEEP_TIME);
//		}
//		catch (InterruptedException e) 
//		{
//			e.printStackTrace();
//		}
		
		List<String> removingKeyList = new LinkedList<String>();
		
		Iterator<String> queryIter = activeQMap.keySet().iterator();
		
		while( queryIter.hasNext() )
		{
			String queryKey = queryIter.next();
			ActiveQueryStorage activeQueryStore = activeQMap.get(queryKey);
			if( SearchAndUpdateDriver.currentRealTime 
								> activeQueryStore.getExpiryUnixTime() )
			{
				// remove the query
				removingKeyList.add(queryKey);
			}
			else
			{
				if( 
				(SearchAndUpdateDriver.currentRealTime - activeQueryStore.getLastSentUnixTime()) >= 
						SearchAndUpdateDriver.queryRefreshTime )
				{
//					System.out.println("Refreshing a query Key "+activeQueryStore.getSearchQueryKey()
//					 +" Curr time "+SearchAndUpdateDriver.currentRealTime 
//					 +" Issue time "+activeQueryStore.getIssueUnixTime()
//					 + " Expiry time "+activeQueryStore.getExpiryUnixTime());
					
					activeQueryStore.updateLastSentUnixTime(
								(long)SearchAndUpdateDriver.currentRealTime);
					sendARefreshQuery(activeQueryStore);
				}
			}	
		}
		
		// now remove queries
		//System.out.println("Num Queries removed "+removingKeyList.size());
		for(int i=0; i<removingKeyList.size(); i++)
		{
			String key = removingKeyList.get(i);
			activeQMap.remove(key);
		}
	}
	
	
	private void sendARefreshQuery( ActiveQueryStorage activeQueryStore )
	{
		String searchQuery = activeQueryStore.getQueryString();
		ExperimentSearchReply searchRep = new ExperimentSearchReply( numSent );
		
		synchronized(numSentLock)
		{
			numSent++;
		}
		
		long queryExpiry = 0;
		csClient.sendSearchQueryWithCallBack
			( searchQuery, queryExpiry, searchRep, getCallBack() );
	}
	
	public SearchStatClass getStatObj()
	{
		//Date date = new Date((long)SearchAndUpdateDriver.currentRealTime*1000L); 
		// *1000 is to convert seconds to milliseconds
		//SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); 
		// the format of your date
		//sdf.setTimeZone(TimeZone.getTimeZone("GMT")); 
		// give a timezone reference for formating (see comment at the bottom
		//String dateFormat = sdf.format(date);
		
		SearchStatClass searchStatClass = new SearchStatClass();
		
		long currTime = System.currentTimeMillis();
		double sendingRate = (numSent*1000.0)/(currTime-expStartTime);
		double systemThpt = (numRecvd*1000.0)/(currTime-expStartTime);
		
//		String str = "SearchId "+searchId+" numSent "+numSent+" numRecvd "
//							+ numRecvd+" avg reply size "+ (sumResultSize/numRecvd)
//							+ " sending rate "+sendingRate
//							+ " system throughput "+systemThpt
//							+ " latency "+(sumSearchLatency/numRecvd)+" ms"
//							+ " result size "+(sumResultSize/numRecvd)
//							+ " numOriginalSearch "+numOriginalSearch
//							+ " avergage LatRange "+(this.sumLatitude/numOriginalSearch)
//							+ " avergage LongRange "+(this.sumLongitude/numOriginalSearch);
		
		searchStatClass.sendingRate = sendingRate;
		searchStatClass.systemThpt = systemThpt;
		searchStatClass.numSent = numSent;
		searchStatClass.numRecvd = numRecvd;
		searchStatClass.latency = (sumSearchLatency/numSearch);
		searchStatClass.avgReplySize = (sumResultSize/numSearch);
		searchStatClass.numOriginalSearch = numOriginalSearch;
		searchStatClass.avgLatRange = (this.sumLatitude/numOriginalSearch);
		searchStatClass.avgLongRange = (this.sumLongitude/numOriginalSearch);
		
		return searchStatClass;
	}
	
	
	public void sendSearchesWhoseTimeHasCome()
	{
		List<WeatherEventStorage> buffaloWeatherList 
							= weatherDataProcess.getBuffaloAreaWeatherEvents();
		
		while( nextIndexToSend < buffaloWeatherList.size() )
		{
			WeatherEventStorage currWeatherEvent 
										= buffaloWeatherList.get(nextIndexToSend);
			if( currWeatherEvent.getIssueUnixTimeStamp() <=  SearchAndUpdateDriver.currentRealTime )
			{
				sendSearchQuery(currWeatherEvent, numSent);
				nextIndexToSend++;
			}
			else
			{
				break;
			}
		}
	}
	
	private void sendSearchQuery(WeatherEventStorage currWeatherEvent, long requestId)
	{
		List<List<GlobalCoordinate>> polygonsList = currWeatherEvent.getListOfPolygons();
		
		for( int i=0; i<polygonsList.size(); i++ )
		{
			List<GlobalCoordinate> polygon =  polygonsList.get(i);
			
			Path2D geoJSONPolygon = new Path2D.Double();
			GlobalCoordinate gCoord = polygon.get(0);
			geoJSONPolygon.moveTo( gCoord.getLatitude(), gCoord.getLongitude() );
			for( int j = 1; j<polygon.size(); ++j )
			{
				gCoord = polygon.get(j);
				geoJSONPolygon.lineTo( gCoord.getLatitude(), 
						gCoord.getLongitude() );
			}
			geoJSONPolygon.closePath();
			Rectangle2D boundingRect = geoJSONPolygon.getBounds2D();
			
			double minLat  = boundingRect.getMinX();
			double maxLat  = boundingRect.getMaxX();
			double minLong = boundingRect.getMinY();
			double maxLong = boundingRect.getMaxY();
			
			this.sumLatitude = this.sumLatitude + (maxLat - minLat);
			this.sumLongitude = this.sumLongitude + (maxLong - minLong);
			
			numOriginalSearch++;
			
			String searchQuery = SearchAndUpdateDriver.latitudeAttr+" >= "+minLat+
				" AND "+SearchAndUpdateDriver.latitudeAttr+" <= "+maxLat
				+" AND "+SearchAndUpdateDriver.longitudeAttr+" >= "+
				minLong+" AND "+SearchAndUpdateDriver.longitudeAttr+" <= "+maxLong;
			
			ExperimentSearchReply searchRep 
							= new ExperimentSearchReply( requestId );
			
			synchronized(numSentLock)
			{
				numSent++;
			}
			
			double div = currWeatherEvent.getDurationInSecs()/SearchAndUpdateDriver.TIME_CONTRACTION_REAL_TIME;
			long queryExpiry = (long) Math.ceil( div);
			csClient.sendSearchQueryWithCallBack
				( searchQuery, queryExpiry, searchRep, this.getCallBack() );
			
			if(!SearchAndUpdateDriver.triggerEnabled)
			{
				String activeQueryKey = currWeatherEvent.getWeatherEventId()+"-"+i;
				ActiveQueryStorage activeQ = new ActiveQueryStorage( activeQueryKey, 
						currWeatherEvent.getIssueUnixTimeStamp(), 
						currWeatherEvent.getExpireUnixTimeStamp(), searchQuery );
				
				this.activeQMap.put(activeQueryKey, activeQ);
			}
		}
	}
	
	public static void main( String[] args ) throws NoSuchAlgorithmException, IOException, InterruptedException
	{
		//csHost = args[0];
		//csPort = Integer.parseInt(args[1]);
		//ContextServiceClient<String> csClient = new ContextServiceClient<String>();
		//IssueSearches issueSearch = new IssueSearches(null, 300, 0);
		//issueSearch.runSearches();
	}
}