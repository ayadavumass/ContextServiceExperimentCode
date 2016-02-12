package edu.umass.cs.mysqlBenchmarking;


import java.util.Random;
/**
 * Updates locations of all users after every 
 * granularityOfGeolocationUpdate
 * @author adipc
 */
public class SearchClass extends AbstractRequestSendingClass implements Runnable
{		
	private final Random queryRand;
	
	public SearchClass()
	{
		super(MySQLBenchmarking.SEARCH_LOSS_TOLERANCE);
		queryRand = new Random();
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
	
	//String query 
	// = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap(geoLocationCurrentLat, geoLocationCurrentLong, "+geoJSONObject.toString()+")";
	private void searchQueryRateControlledRequestSender() throws Exception
	{	
		// as it is per ms
		double reqspms = MySQLBenchmarking.searchRequestsps/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		while( ( (System.currentTimeMillis() - expStartTime) < MySQLBenchmarking.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
//				JSONObject queryGeoJSON = 
//						weatherAlertsArray.get( queryRand.nextInt(weatherAlertsArray.size() ) );
				int beg1 = this.queryRand.nextInt(1400);
		    	int end1 = beg1+this.queryRand.nextInt(1500 - beg1-3);
		    	
		    	int beg2 = this.queryRand.nextInt(1400);
		    	int end2 = beg2+this.queryRand.nextInt(1500 - beg2-3);
		    	
				String selectTableSQL = "SELECT nodeGUID from "+MySQLBenchmarking.tableName+" WHERE "
				+ "( value1 >= "+beg1 +" AND value1 < "+end1+" AND "
				+ " value2 >= "+beg2 +" AND value2 < "+end2+" )";
				sendQueryMessage(selectTableSQL);
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
				int beg1 = this.queryRand.nextInt(1400);
		    	int end1 = beg1+this.queryRand.nextInt(1500 - beg1-3);
		    	
		    	int beg2 = this.queryRand.nextInt(1400);
		    	int end2 = beg2+this.queryRand.nextInt(1500 - beg2-3);
		    	
				String selectTableSQL = "SELECT nodeGUID from "+MySQLBenchmarking.tableName+" WHERE "
				+ "( value1 >= "+beg1 +" AND value1 < "+end1+" AND "
				+ " value2 >= "+beg2 +" AND value2 < "+end2+" )";
				sendQueryMessage(selectTableSQL);
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
		MySQLBenchmarking.taskES.execute(searchTask);
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