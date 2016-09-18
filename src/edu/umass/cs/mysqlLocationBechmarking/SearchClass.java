package edu.umass.cs.mysqlLocationBechmarking;

import java.util.Random;

/**
 * Updates locations of all users after every 
 * granularityOfGeolocationUpdate
 * @author adipc
 */
public class SearchClass extends AbstractRequestSendingClass
{
	private final Random queryRand;
	private double sumResultSize = 0.0;
	private double sumTime = 0.0;
	
	public SearchClass()
	{
		super(MySQLThroughputBenchmarking.SEARCH_LOSS_TOLERANCE);
		queryRand = new Random();
	}
	
	@Override
	public void run()
	{
		try
		{
			this.startExpTime();
			searchQueryRateControlledRequestSender();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	// String query 
	// = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE 
	// GeojsonOverlap(geoLocationCurrentLat, geoLocationCurrentLong, 
	// "+geoJSONObject.toString()+")";
	private void searchQueryRateControlledRequestSender() throws Exception
	{
		// as it is per ms
		double reqspms = MySQLThroughputBenchmarking.requestsps/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		while( ( (System.currentTimeMillis() - expStartTime)
				< MySQLThroughputBenchmarking.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
//				sendQueryMessage(selectTableSQL);
				sendQueryMessageWithSmallRanges();
				//sendQueryMessage();
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
//				sendQueryMessage(selectTableSQL);
				sendQueryMessageWithSmallRanges();
				//sendQueryMessage();
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
	
	private void sendQueryMessageWithSmallRanges()
	{	
		switch(MySQLThroughputBenchmarking.requestType)
		{
			case MySQLThroughputBenchmarking.SINGLE_ATTR_REQ:
			{
				String searchQuery = getASingleAttributeQuery();
				SearchTask searchTask = new SearchTask( searchQuery, this );
				MySQLThroughputBenchmarking.taskES.execute(searchTask);
				break;
			}
			case MySQLThroughputBenchmarking.DOUBLE_ATTR_REQ:
			{
				String searchQuery = getAMultipleAttributeQuery();
				SearchTask searchTask = new SearchTask( searchQuery, this );
				MySQLThroughputBenchmarking.taskES.execute(searchTask);
				break;
			}
		}
	}
	
	private String getASingleAttributeQuery()
	{
		String searchQuery = "";
		if(MySQLThroughputBenchmarking.getOnlyCount)
		{
			searchQuery 
				= "SELECT COUNT(nodeGUID) AS RESULT_SIZE FROM "+MySQLThroughputBenchmarking.dataTableName+" WHERE ( ";
		}
		else
		{
			searchQuery
				= "SELECT nodeGUID FROM "+MySQLThroughputBenchmarking.dataTableName+" WHERE ( ";
		}
		
		
		boolean latOrLong = queryRand.nextBoolean();
		if(latOrLong)
		{
			// latitude case
			String attrName = "latitude";
			double attrMin 
				= MySQLThroughputBenchmarking.LAT_MIN
					+queryRand.nextDouble()*(MySQLThroughputBenchmarking.LAT_MAX - 
					MySQLThroughputBenchmarking.LAT_MIN);
		
			// querying 10 % of domain
			double predLength 
				= ( MySQLThroughputBenchmarking.predicateLength
						* ( MySQLThroughputBenchmarking.LAT_MAX 
						- MySQLThroughputBenchmarking.LAT_MIN ) );
	
			double attrMax = attrMin + predLength;
			//		double latitudeMax = latitudeMin 
			//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
			// making it curcular
			if( attrMax > MySQLThroughputBenchmarking.LAT_MAX )
			{
				attrMax = MySQLThroughputBenchmarking.LAT_MAX;
			}
			
			searchQuery = searchQuery + " ( "+attrName +" >= "+attrMin +" AND " 
					+attrName +" <= "+attrMax+" ) )";
		}
		else
		{
			// latitude case
			String attrName = "longitude";
			double attrMin 
				= MySQLThroughputBenchmarking.LONG_MIN
					+ queryRand.nextDouble()*(MySQLThroughputBenchmarking.LONG_MAX - 
					  MySQLThroughputBenchmarking.LONG_MIN);
		
			// querying 10 % of domain
			double predLength 
				= ( MySQLThroughputBenchmarking.predicateLength
						* ( MySQLThroughputBenchmarking.LONG_MAX 
						- MySQLThroughputBenchmarking.LONG_MIN ) );
	
			double attrMax = attrMin + predLength;
			//		double latitudeMax = latitudeMin 
			//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
			// making it curcular
			if( attrMax > MySQLThroughputBenchmarking.LONG_MAX )
			{
				attrMax = MySQLThroughputBenchmarking.LONG_MAX;
			}
			
			searchQuery = searchQuery + " ( "+attrName +" >= "+attrMin +" AND " 
					+attrName +" <= "+attrMax+" ) )";
		}
		
		return searchQuery;
	}
	
	private String getAMultipleAttributeQuery()
	{
		String searchQuery = "";
		if(MySQLThroughputBenchmarking.getOnlyCount)
		{
			searchQuery
				= "SELECT COUNT(nodeGUID) AS RESULT_SIZE FROM "+MySQLThroughputBenchmarking.dataTableName+" WHERE ( ";
		}
		else
		{
			searchQuery
				= "SELECT nodeGUID FROM "+MySQLThroughputBenchmarking.dataTableName+" WHERE ( ";
		}
		
		
		// latitude case
		String attrName = "latitude";
		double attrMin 
			= MySQLThroughputBenchmarking.LAT_MIN
				+ queryRand.nextDouble()*(MySQLThroughputBenchmarking.LAT_MAX - 
					MySQLThroughputBenchmarking.LAT_MIN);
				
		// querying 10 % of domain
		double predLength 
			= ( MySQLThroughputBenchmarking.predicateLength
					* ( MySQLThroughputBenchmarking.LAT_MAX 
					- MySQLThroughputBenchmarking.LAT_MIN ) );
		
		double attrMax = attrMin + predLength;
		//		double latitudeMax = latitudeMin 
		//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
		// making it curcular
		if( attrMax > MySQLThroughputBenchmarking.LAT_MAX )
		{
			attrMax = MySQLThroughputBenchmarking.LAT_MAX;
		}
		
		searchQuery = searchQuery + " ( "+attrName +" >= "+attrMin +" AND " 
				+attrName +" <= "+attrMax+" ) ";
		
		
		attrName = "longitude";
		attrMin 
			= MySQLThroughputBenchmarking.LONG_MIN
				+ queryRand.nextDouble()*(MySQLThroughputBenchmarking.LONG_MAX - 
					MySQLThroughputBenchmarking.LONG_MIN);
				
		// querying 10 % of domain
		predLength 
			= ( MySQLThroughputBenchmarking.predicateLength
					* ( MySQLThroughputBenchmarking.LONG_MAX 
					- MySQLThroughputBenchmarking.LONG_MIN ) );
			
		attrMax = attrMin + predLength;
		//		double latitudeMax = latitudeMin 
		//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
		// making it curcular
		if( attrMax > MySQLThroughputBenchmarking.LONG_MAX )
		{
			attrMax = MySQLThroughputBenchmarking.LONG_MAX;
		}
		
		searchQuery = searchQuery + " AND ( "+attrName +" >= "+attrMin +" AND " 
				+attrName +" <= "+attrMax+" ) ) ";
		
		return searchQuery;
	}
	
	public double getAvgResultSize()
	{
		return sumResultSize/numRecvd;
	}
	
	public double getAvgTime()
	{
		return this.sumTime/numRecvd;
	}
	
	public double getSearchCapacity()
	{
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		return sysThrput;
		//System.out.println("Search result:Goodput "+sysThrput);
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
			//System.out.println("Search reply recvd size "+resultSize+" time taken "+timeTaken+
			//		" numSent "+numSent+" numRecvd "+numRecvd);
			//if(currNumReplyRecvd == currNumReqSent)
			sumResultSize = sumResultSize + resultSize;
			sumTime = sumTime + timeTaken;
			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{	
				waitLock.notify();
			}
		}
	}
}