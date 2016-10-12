package edu.umass.cs.weatherExpClient;

import java.util.Random;

import org.json.JSONArray;

public class UniformQueryClass extends AbstractRequestSendingClass implements Runnable
{
	private final Random latLowerValRand;
	private final Random latPredLenRand;
	
	private final Random longLowerValRand;
	private final Random longPredLenRand;
	
	public UniformQueryClass()
	{
		super(WeatherAndMobilityBoth.SEARCH_LOSS_TOLERANCE);
		latLowerValRand = new Random(WeatherAndMobilityBoth.myID);
		latPredLenRand = new Random(WeatherAndMobilityBoth.myID);
		
		longLowerValRand = new Random(WeatherAndMobilityBoth.myID);
		longPredLenRand = new Random(WeatherAndMobilityBoth.myID);
	}
		
	@Override
	public void run()
	{
		try
		{
			this.startExpTime();
			weatherQueryRateControlledRequestSender();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
		
	//String query 
	// = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap(geoLocationCurrentLat, geoLocationCurrentLong, "+geoJSONObject.toString()+")";
	private void weatherQueryRateControlledRequestSender() throws Exception
	{	
		// as it is per ms
		double reqspms = WeatherAndMobilityBoth.searchQueryRate/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		while( ( (System.currentTimeMillis() - expStartTime) < WeatherAndMobilityBoth.EXPERIMENT_TIME ) )
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
		double latitudeMin = WeatherAndMobilityBoth.LATITUDE_MIN 
				+latLowerValRand.nextDouble()*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
		
		double predLength 
		= (latPredLenRand.nextDouble()*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN));
		predLength=predLength/10.0;
		
		double latitudeMax = latitudeMin + predLength;
//		double latitudeMax = latitudeMin 
//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
		// making it curcular
		if(latitudeMax > WeatherAndMobilityBoth.LATITUDE_MAX)
		{
			double diff = latitudeMax - WeatherAndMobilityBoth.LATITUDE_MAX;
			latitudeMax = WeatherAndMobilityBoth.LATITUDE_MIN + diff;
		}
		
		
		double longitudeMin = WeatherAndMobilityBoth.LONGITUDE_MIN 
				+longLowerValRand.nextDouble()*(WeatherAndMobilityBoth.LONGITUDE_MAX - WeatherAndMobilityBoth.LONGITUDE_MIN);
		
		predLength = (longPredLenRand.nextDouble()*(WeatherAndMobilityBoth.LONGITUDE_MAX - WeatherAndMobilityBoth.LONGITUDE_MIN));
		predLength=predLength/10.0;
		
		double longitudeMax = longitudeMin + predLength;
//		double longitudeMax = WeatherAndMobilityBoth.LONGITUDE_MIN 
//				+queryRand.nextDouble()*(WeatherAndMobilityBoth.LONGITUDE_MAX - WeatherAndMobilityBoth.LONGITUDE_MIN);
//		double longitudeMax = longitudeMin 
//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LONGITUDE_MAX - WeatherAndMobilityBoth.LONGITUDE_MIN);
		
		if( longitudeMax > WeatherAndMobilityBoth.LONGITUDE_MAX )
		{
			double diff = longitudeMax - WeatherAndMobilityBoth.LONGITUDE_MAX;
			longitudeMax = WeatherAndMobilityBoth.LONGITUDE_MIN + diff;
		}

		String searchQuery
			= "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE "
				+ "geoLocationCurrentLat >= "+latitudeMin +" AND geoLocationCurrentLat <= "+latitudeMax 
				+ " AND "
				+ "geoLocationCurrentLong >= "+longitudeMin+" AND geoLocationCurrentLong <= "+longitudeMax;
		
//			String searchQuery
//				= "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap(geoLocationCurrentLat, geoLocationCurrentLong, "+queryGeoJSON.toString()+")";
		SearchTask searchTask = new SearchTask( searchQuery, new JSONArray(), this );
		WeatherAndMobilityBoth.taskES.execute(searchTask);
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