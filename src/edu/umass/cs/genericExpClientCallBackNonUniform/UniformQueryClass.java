package edu.umass.cs.genericExpClientCallBackNonUniform;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

public class UniformQueryClass extends AbstractRequestSendingClass implements Runnable
{
	private final Random searchQueryRand;
	
	private long sumResultSize				= 0;
	private long sumSearchLatency			= 0;
	private long numSearchesRecvd			= 0;
	
	public UniformQueryClass()
	{
		super(SearchAndUpdateDriver.SEARCH_LOSS_TOLERANCE);
		searchQueryRand = new Random((SearchAndUpdateDriver.myID+1)*500);
	}
	
	@Override
	public void run()
	{
		try
		{
			this.startExpTime();
			searchQueryRateControlledRequestSender();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void searchQueryRateControlledRequestSender() throws Exception
	{
		// as it is per ms
		double reqsps = SearchAndUpdateDriver.searchQueryRate;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqsps;
		
		while( ( (System.currentTimeMillis() - expStartTime) 
				< SearchAndUpdateDriver.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				sendQueryMessage(numSent);
				numSent++;
			}
			currTime = System.currentTimeMillis();
			
			double timeElapsed = ((currTime- expStartTime)*1.0);
			double numberShouldBeSentByNow = timeElapsed*reqsps/1000.0;
			double needsToBeSentBeforeSleep = numberShouldBeSentByNow - numSent;
			if(needsToBeSentBeforeSleep > 0)
			{
				needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
			}
			
			for(int i=0;i<needsToBeSentBeforeSleep;i++)
			{
				sendQueryMessage(numSent);
				numSent++;
			}
			Thread.sleep(1000);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Search eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Search result:Goodput "+sysThrput);
		
		double avgResultSize = 0;
		if( this.numSearchesRecvd > 0 )
		{
			avgResultSize = (sumResultSize/this.numSearchesRecvd);
		}
		
		System.out.println("Both result:Goodput "+sysThrput+" average resultsize "
										+avgResultSize);
	}
	
	
	private void sendQueryMessage(long reqIdNum)
	{
		String searchQuery = "";
		
		HashMap<String, Boolean> distinctAttrMap 
			= pickDistinctAttrs( SearchAndUpdateDriver.numAttrsInQuery, 
				SearchAndUpdateDriver.numAttrs, searchQueryRand );
		
		Iterator<String> attrIter = distinctAttrMap.keySet().iterator();
	
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			double attrMin = SearchAndUpdateDriver.ATTR_MIN
					+searchQueryRand.nextDouble()*(SearchAndUpdateDriver.ATTR_MAX 
							- SearchAndUpdateDriver.ATTR_MIN);
		
			// querying 10 % of domain
			double predLength 
				= ( SearchAndUpdateDriver.predicateLengthRatio
						*(SearchAndUpdateDriver.ATTR_MAX - SearchAndUpdateDriver.ATTR_MIN) );
		
			double attrMax = attrMin + predLength;
			//		double latitudeMax = latitudeMin 
			//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
			// making it curcular
			if( attrMax > SearchAndUpdateDriver.ATTR_MAX )
			{
				double diff = attrMax - SearchAndUpdateDriver.ATTR_MAX;
				attrMax = SearchAndUpdateDriver.ATTR_MIN + diff;
			}
			// last so no AND
			if( !attrIter.hasNext() )
			{
				searchQuery = searchQuery + " "+attrName+" >= "+attrMin+" AND "+attrName
						+" <= "+attrMax;
			}
			else
			{
				searchQuery = searchQuery + " "+attrName+" >= "+attrMin+" AND "+attrName
					+" <= "+attrMax+" AND ";
			}
		}
		
		ExperimentSearchReply searchRep 
			= new ExperimentSearchReply( reqIdNum );
		
		SearchAndUpdateDriver.csClient.sendSearchQueryWithCallBack
			( searchQuery, SearchAndUpdateDriver.queryExpiryTime, searchRep, this.getCallBack() );
	}
	
	
	private HashMap<String, Boolean> pickDistinctAttrs( int numAttrsToPick, 
			int totalAttrs, Random randGen )
	{
		HashMap<String, Boolean> hashMap = new HashMap<String, Boolean>();
		int currAttrNum = 0;
		while(hashMap.size() != numAttrsToPick)
		{
			if(SearchAndUpdateDriver.numAttrs == SearchAndUpdateDriver.numAttrsInQuery)
			{
				String attrName = "attr"+currAttrNum;
				hashMap.put(attrName, true);
				currAttrNum++;
			}
			else
			{
				currAttrNum = randGen.nextInt(SearchAndUpdateDriver.numAttrs);
				String attrName = "attr"+currAttrNum;
				hashMap.put(attrName, true);
			}
		}
		return hashMap;
	}
	
	public double getAverageSearchLatency()
	{
		return (this.numSearchesRecvd>0)?sumSearchLatency/this.numSearchesRecvd:0;
	}
	
	
	public long getNumSearchesRecvd()
	{
		return this.numSearchesRecvd;
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
//			System.out.println("Search reply recvd size "+resultSize+" time taken "+timeTaken+
//					" numSent "+numSent+" numRecvd "+numRecvd);
			
			this.numSearchesRecvd++;
			sumResultSize = sumResultSize + resultSize;
//			System.out.println("Search reply recvd size "+resultSize+" time taken "
//					+timeTaken+" numSent "+numSent+" numRecvd "+numRecvd);
			//if(currNumReplyRecvd == currNumReqSent)
			this.sumSearchLatency = this.sumSearchLatency + timeTaken;
			
			//if(currNumReplyRecvd == currNumReqSent)
			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				waitLock.notify();
			}
		}
	}
		
//		private void sendQueryMessage()
//		{
//			double latitudeMin = WeatherAndMobilityBoth.LATITUDE_MIN 
//					+queryRand.nextDouble()*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
//			
//			double latitudeMax = latitudeMin 
//					+queryRand.nextDouble()*(WeatherAndMobilityBoth.LATITUDE_MAX - latitudeMin);
////			double latitudeMax = latitudeMin 
////					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
//			
//			
//			double longitudeMin = WeatherAndMobilityBoth.LONGITUDE_MIN 
//					+queryRand.nextDouble()*(WeatherAndMobilityBoth.LONGITUDE_MAX - WeatherAndMobilityBoth.LONGITUDE_MIN);
//			double longitudeMax = longitudeMin 
//					+queryRand.nextDouble()*(WeatherAndMobilityBoth.LONGITUDE_MAX - longitudeMin);
////			double longitudeMax = longitudeMin 
////					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LONGITUDE_MAX - WeatherAndMobilityBoth.LONGITUDE_MIN);
//
//			String searchQuery
//				= "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE "
//					+ "geoLocationCurrentLat >= "+latitudeMin +" AND geoLocationCurrentLat <= "+latitudeMax 
//					+ " AND "
//					+ "geoLocationCurrentLong >= "+longitudeMin+" AND geoLocationCurrentLong <= "+longitudeMax;
//			
////			String searchQuery
////				= "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap(geoLocationCurrentLat, geoLocationCurrentLong, "+queryGeoJSON.toString()+")";
//			SearchTask searchTask = new SearchTask( searchQuery, new JSONArray(), this );
//			WeatherAndMobilityBoth.taskES.execute(searchTask);
//		}
}