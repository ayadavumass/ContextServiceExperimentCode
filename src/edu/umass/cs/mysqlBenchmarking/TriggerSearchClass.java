package edu.umass.cs.mysqlBenchmarking;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;


import edu.umass.cs.contextservice.utils.Utils;


public class TriggerSearchClass extends AbstractRequestSendingClass
{
	private final Random queryRand;
	private double sumResultSize = 0.0;
	private double sumTime = 0.0;
	
	public TriggerSearchClass()
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
		double reqspms = 100.0/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		while(  numSent < MySQLThroughputBenchmarking.numOfSearchQueries )
		{
			for( int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				insertIntoSubspaceTriggerDataInfo();
				//sendQueryMessage();
				numSent++;
			}
			currTime = System.currentTimeMillis();
			
			double timeElapsed = ((currTime- expStartTime)*1.0);
			double numberShouldBeSentByNow = timeElapsed*reqspms;
			double needsToBeSentBeforeSleep = numberShouldBeSentByNow - numSent;
			if( needsToBeSentBeforeSleep > 0 )
			{
				needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
			}
			
			for( int i=0;i<needsToBeSentBeforeSleep;i++ )
			{
				insertIntoSubspaceTriggerDataInfo();
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
	
	/**
	 * Inserts trigger info on a query into the table
	 * @param subspaceNum
	 * @param subspaceVector
	 */
	public void insertIntoSubspaceTriggerDataInfo()
	{
		String userIP = "127.0.0.1";
		int userPort = 5000+ queryRand.nextInt(40000);
		long expiryTimeFromNow = 60000;
		String str = "query"+numSent;
		String groupGUID = MySQLThroughputBenchmarking.getSHA1(str);
		
		HashMap<String, Boolean> distinctAttrMap 
			= pickDistinctAttrs( MySQLThroughputBenchmarking.numAttrsInQuery, 
					MySQLThroughputBenchmarking.numAttrs, queryRand );
		
		try
		{
			String hexIP = Utils.bytArrayToHex(InetAddress.getByName(userIP).getAddress());	
			
			String insertTableSQL = " INSERT INTO "
					+ MySQLThroughputBenchmarking.triggerTableName 
					+ " ( groupGUID, userIP, userPort , expiryTime ";
			
			Iterator<String> qattrIter = distinctAttrMap.keySet().iterator();
			while( qattrIter.hasNext() )
			{
				String qattrName = qattrIter.next();
				String lowerAtt = "lower"+qattrName;
				String upperAtt = "upper"+qattrName;
				insertTableSQL = insertTableSQL + ", "+lowerAtt+" , "+upperAtt;
			}
			
			insertTableSQL = insertTableSQL + " ) VALUES ( X'"+groupGUID+"', "+
							 " X'"+hexIP+"', "+userPort+" , "+expiryTimeFromNow+" ";
			
			// assuming the order of iterator over attributes to be same in above and here
			qattrIter = distinctAttrMap.keySet().iterator();
			while( qattrIter.hasNext() )
			{
				String qattrName = qattrIter.next();
				
				double attrMin 
					= MySQLThroughputBenchmarking.ATTR_MIN
						+queryRand.nextDouble()*(MySQLThroughputBenchmarking.ATTR_MAX - 
						MySQLThroughputBenchmarking.ATTR_MIN);
			
				// querying 10 % of domain
				double predLength 
					= (MySQLThroughputBenchmarking.predicateLength
						*(MySQLThroughputBenchmarking.ATTR_MAX 
								- MySQLThroughputBenchmarking.ATTR_MIN)) ;
			
				double attrMax = attrMin + predLength;
				//		double latitudeMax = latitudeMin 
				//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
				// making it curcular
				if( attrMax > MySQLThroughputBenchmarking.ATTR_MAX )
				{
					double diff = attrMax - MySQLThroughputBenchmarking.ATTR_MAX;
					attrMax = MySQLThroughputBenchmarking.ATTR_MIN + diff;
				}
				
				String lowerBound = attrMin+"";
				String upperBound = attrMax+"";
				
				insertTableSQL = insertTableSQL + " , "+lowerBound+" , "+ upperBound;
				
			}
			insertTableSQL = insertTableSQL + " ) ";
			
			TriggerSearchTask searchTask = new TriggerSearchTask( insertTableSQL, this );
			MySQLThroughputBenchmarking.taskES.execute(searchTask);
		}  
		catch (UnknownHostException e)
		{
			e.printStackTrace();
		}
	}
	
	
	private HashMap<String, Boolean> pickDistinctAttrs( int numAttrsToPick, 
			int totalAttrs, Random randGen )
	{
		HashMap<String, Boolean> hashMap = new HashMap<String, Boolean>();
		int currAttrNum = 0;
		while(hashMap.size() != numAttrsToPick)
		{
			if(MySQLThroughputBenchmarking.numAttrs == MySQLThroughputBenchmarking.numAttrsInQuery)
			{
				String attrName = "attr"+currAttrNum;
				hashMap.put(attrName, true);				
				currAttrNum++;
			}
			else
			{
				currAttrNum = randGen.nextInt(MySQLThroughputBenchmarking.numAttrs);
				String attrName = "attr"+currAttrNum;
				hashMap.put(attrName, true);
			}
		}
		return hashMap;
	}
	
	public double getAvgResultSize()
	{
		return sumResultSize/numRecvd;
	}
	
	public double getAvgTime()
	{
		return this.sumTime/numRecvd;
	}
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			sumTime = sumTime + timeTaken;
			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				waitLock.notify();
			}
		}
	}
	
	@Override
	public void incrementSearchNumRecvd(int resultSize, long timeTaken)
	{
	}
}