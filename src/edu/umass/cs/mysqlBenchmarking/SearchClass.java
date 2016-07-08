package edu.umass.cs.mysqlBenchmarking;

import java.util.Random;

/**
 * Updates locations of all users after every 
 * granularityOfGeolocationUpdate
 * @author adipc
 */
public class SearchClass extends AbstractRequestSendingClass
{
	private final Random queryRand;
	
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
	
	//String query 
	// = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap(geoLocationCurrentLat, geoLocationCurrentLong, "+geoJSONObject.toString()+")";
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
//				JSONObject queryGeoJSON = 
//						weatherAlertsArray.get( queryRand.nextInt(weatherAlertsArray.size() ) );
//				int beg1 = this.queryRand.nextInt(1400);
//		    	int end1 = beg1+this.queryRand.nextInt(1500 - beg1-3);
//		    	
//		    	int beg2 = this.queryRand.nextInt(1400);
//		    	int end2 = beg2+this.queryRand.nextInt(1500 - beg2-3);
//		    	
//				String selectTableSQL = "SELECT nodeGUID from "+MySQLThroughputBenchmarking.tableName+" WHERE "
//				+ "( value1 >= "+beg1 +" AND value1 < "+end1+" AND "
//				+ " value2 >= "+beg2 +" AND value2 < "+end2+" )";
//				sendQueryMessage(selectTableSQL);
				sendQueryMessageWithSmallRanges();
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
//				int beg1 = this.queryRand.nextInt(1400);
//		    	int end1 = beg1+this.queryRand.nextInt(1500 - beg1-3);
//		    	
//		    	int beg2 = this.queryRand.nextInt(1400);
//		    	int end2 = beg2+this.queryRand.nextInt(1500 - beg2-3);
//		    	
//				String selectTableSQL = "SELECT nodeGUID from "+MySQLThroughputBenchmarking.tableName+" WHERE "
//				+ "( value1 >= "+beg1 +" AND value1 < "+end1+" AND "
//				+ " value2 >= "+beg2 +" AND value2 < "+end2+" )";
//				sendQueryMessage(selectTableSQL);
				sendQueryMessageWithSmallRanges();
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
	
	private void sendQueryMessage(String mysqlQuery)
	{
		SearchTask searchTask = new SearchTask( mysqlQuery, this );
		MySQLThroughputBenchmarking.taskES.execute(searchTask);
	}
	
	private void sendQueryMessage()
	{
		String searchQuery
			= "SELECT nodeGUID FROM "+MySQLThroughputBenchmarking.tableName+" WHERE ";
//			+ "geoLocationCurrentLat >= "+latitudeMin +" AND geoLocationCurrentLat <= "+latitudeMax 
//			+ " AND "
//			+ "geoLocationCurrentLong >= "+longitudeMin+" AND geoLocationCurrentLong <= "+longitudeMax;
		
		int randAttrNum = -1;
		for( int i=0; i<MySQLThroughputBenchmarking.numAttrsInQuery; i++)
		{
			// if num attrs and num in query are same then send query on all attrs
			if(MySQLThroughputBenchmarking.numAttrs == MySQLThroughputBenchmarking.numAttrsInQuery)
			{
				randAttrNum++;
			}
			else
			{
				randAttrNum = queryRand.nextInt(MySQLThroughputBenchmarking.numAttrs);
			}				
			
			String attrName = "attr"+randAttrNum;
			double attrMin 
				= 1
				+queryRand.nextDouble()*(MySQLThroughputBenchmarking.ATTR_MAX - MySQLThroughputBenchmarking.ATTR_MIN);
			
			double predLength 
				= (queryRand.nextDouble()*(MySQLThroughputBenchmarking.ATTR_MAX - MySQLThroughputBenchmarking.ATTR_MIN));
			
			double attrMax = attrMin + predLength;
			//		double latitudeMax = latitudeMin 
			//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
			// making it curcular
//			if( attrMax > MySQLThroughputBenchmarking.ATTR_MAX )
//			{
////				double diff = attrMax - MySQLThroughputBenchmarking.ATTR_MAX;
////				attrMax = MySQLThroughputBenchmarking.ATTR_MIN + diff;
//			}
			
			// last so no AND
			if(i == (MySQLThroughputBenchmarking.numAttrsInQuery-1))
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
//		SearchTask searchTask = new SearchTask( searchQuery, new JSONArray(), this );
//		SearchAndUpdateDriver.taskES.execute(searchTask);
		
		SearchTask searchTask = new SearchTask( searchQuery, this );
		MySQLThroughputBenchmarking.taskES.execute(searchTask);
		
//		ExperimentSearchReply searchRep 
//					= new ExperimentSearchReply( reqIdNum );
//		SearchAndUpdateDriver.csClient.sendSearchQueryWithCallBack
//					(searchQuery, 300000, searchRep, this.getCallBack());
	}
	
	private void sendQueryMessageWithSmallRanges()
	{
		String searchQuery
			= "SELECT nodeGUID FROM "+MySQLThroughputBenchmarking.tableName+" WHERE ";
		
		int randAttrNum = -1;
		for( int i=0; i<MySQLThroughputBenchmarking.numAttrsInQuery; i++)
		{
			// if num attrs and num in query are same then send query on all attrs
			if(MySQLThroughputBenchmarking.numAttrs == MySQLThroughputBenchmarking.numAttrsInQuery)
			{
				randAttrNum++;
			}
			else
			{
				randAttrNum = queryRand.nextInt(MySQLThroughputBenchmarking.numAttrs);
			}
			
			String attrName = "attr"+randAttrNum;
			double attrMin 
				= MySQLThroughputBenchmarking.ATTR_MIN
				+queryRand.nextDouble()*(MySQLThroughputBenchmarking.ATTR_MAX - MySQLThroughputBenchmarking.ATTR_MIN);
			
			// querying 10 % of domain
			double predLength 
				= (0.1*(MySQLThroughputBenchmarking.ATTR_MAX - MySQLThroughputBenchmarking.ATTR_MIN)) ;
			
			double attrMax = attrMin + predLength;
			//		double latitudeMax = latitudeMin 
			//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
			// making it curcular
//			if( attrMax > MySQLThroughputBenchmarking.ATTR_MAX )
//			{
//				double diff = attrMax - MySQLThroughputBenchmarking.ATTR_MAX;
//				attrMax = MySQLThroughputBenchmarking.ATTR_MIN + diff;
//			}
			// last so no AND
			if( i == (MySQLThroughputBenchmarking.numAttrsInQuery-1) )
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
		
//		ExperimentSearchReply searchRep 
//				= new ExperimentSearchReply( reqIdNum );
//		SearchAndUpdateDriver.csClient.sendSearchQueryWithCallBack
//			(searchQuery, 300000, searchRep, this.getCallBack());
		
		SearchTask searchTask = new SearchTask( searchQuery, this );
		MySQLThroughputBenchmarking.taskES.execute(searchTask);
//		SearchTask searchTask = new SearchTask( searchQuery, new JSONArray(), this );
//		SearchAndUpdateDriver.taskES.execute(searchTask);
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
}