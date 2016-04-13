package edu.umass.cs.privacyExp;

import java.util.Random;

import org.json.JSONArray;

public class UniformQueryClass extends AbstractRequestSendingClass implements Runnable
{
	private final Random searchQueryRand;
	
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
		double reqspms = SearchAndUpdateDriver.searchQueryRate/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		while( ( (System.currentTimeMillis() - expStartTime) < SearchAndUpdateDriver.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				sendQueryMessage();
				numSent++;
			}
			currTime = System.currentTimeMillis();
			
			double timeElapsed = ((currTime- expStartTime)*1.0);
			double numberShouldBeSentByNow = timeElapsed*reqspms;
			double needsToBeSentBeforeSleep = numberShouldBeSentByNow - numSent;
			if(needsToBeSentBeforeSleep > 0)
			{
				needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
			}
			
			for(int i=0;i<needsToBeSentBeforeSleep;i++)
			{
				sendQueryMessage();
				numSent++;
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Search eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Search result:Goodput "+sysThrput);
	}
	
	private void sendQueryMessage()
	{
		String searchQuery
			= "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE ";
//			+ "geoLocationCurrentLat >= "+latitudeMin +" AND geoLocationCurrentLat <= "+latitudeMax 
//			+ " AND "
//			+ "geoLocationCurrentLong >= "+longitudeMin+" AND geoLocationCurrentLong <= "+longitudeMax;
		int randAttrNum = -1;
		for( int i=0;i<SearchAndUpdateDriver.numAttrsInQuery;i++)
		{
			// if num attrs and num in query are same then send query on all attrs
			if(SearchAndUpdateDriver.numAttrs == SearchAndUpdateDriver.numAttrsInQuery)
			{
				randAttrNum++;
			}
			else
			{
				randAttrNum = searchQueryRand.nextInt(SearchAndUpdateDriver.numAttrs);
			}
			
			String attrName = SearchAndUpdateDriver.attrPrefix+randAttrNum;
			double attrMin 
				= SearchAndUpdateDriver.ATTR_MIN
				+searchQueryRand.nextDouble()*(SearchAndUpdateDriver.ATTR_MAX - SearchAndUpdateDriver.ATTR_MIN);
			
			double predLength 
				= (searchQueryRand.nextDouble()*(SearchAndUpdateDriver.ATTR_MAX - SearchAndUpdateDriver.ATTR_MIN));
			
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
			if(i == (SearchAndUpdateDriver.numAttrsInQuery-1))
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
		SearchTask searchTask = new SearchTask( searchQuery, new JSONArray(), this );
		SearchAndUpdateDriver.taskES.execute(searchTask);
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
			System.out.println("Search reply recvd size "+resultSize+" time taken "+timeTaken+
					" numSent "+numSent+" numRecvd "+numRecvd);
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