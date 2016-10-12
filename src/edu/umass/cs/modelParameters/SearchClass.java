package edu.umass.cs.modelParameters;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;
import java.util.Vector;

/**
 * Updates locations of all users after every
 * granularityOfGeolocationUpdate.
 * @author adipc
 */
public class SearchClass extends AbstractRequestSendingClass implements Runnable
{
	private final Random queryRand;
	private final Vector<String> subspaceQueries;
	
	public SearchClass()
	{
		super(ThroughputMeasure.SEARCH_LOSS_TOLERANCE);
		queryRand = new Random();
		subspaceQueries = new Vector<String>();
		readQueriesFile();
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
		double reqspms = ThroughputMeasure.searchRequestsps/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		while( ( (System.currentTimeMillis() - expStartTime) < ThroughputMeasure.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				//String mysqlQuery = generateMySQLQuery(ThroughputMeasure.tableName);
				int index = queryRand.nextInt( subspaceQueries.size() );
				String mysqlQuery = subspaceQueries.get(index);
				
				sendQueryMessage(mysqlQuery);
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
				//String mysqlQuery = generateMySQLQuery(ThroughputMeasure.tableName);
				int index = queryRand.nextInt( subspaceQueries.size() );
				String mysqlQuery = subspaceQueries.get(index);
				
				sendQueryMessage(mysqlQuery);
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
		ThroughputMeasure.taskES.execute(searchTask);
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
	
	private void readQueriesFile()
	{
		BufferedReader br = null;
		
		try
		{
			String searchQueryFileName = "serv"+ThroughputMeasure.nodeId+"SubspaceQueries.txt";
			String sCurrentLine;
			br = new BufferedReader
					(new FileReader(searchQueryFileName));
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				String[] parsed = sCurrentLine.split(" ");
				String mysqlQuery = "";
				
				for(int i=2; i<parsed.length; i++)
				{
					mysqlQuery = mysqlQuery + parsed[i]+" ";
				}
				
				subspaceQueries.add(mysqlQuery);
			}
		} catch (IOException e)
		{
			e.printStackTrace();
		} finally
		{
			try
			{
				if ( br != null )
					br.close();
			} catch (IOException ex)
			{
				ex.printStackTrace();
			}
		}
	}
	
	/*private String generateMySQLQuery(String tableName)
	{
		double latitudeMin = ThroughputMeasure.LATITUDE_MIN 
				+queryRand.nextDouble()*(ThroughputMeasure.LATITUDE_MAX - ThroughputMeasure.LATITUDE_MIN);
		
		double predLength 
			= (queryRand.nextDouble()*(ThroughputMeasure.LATITUDE_MAX - ThroughputMeasure.LATITUDE_MIN));
		
		double latitudeMax = latitudeMin + predLength;
		
		// making it curcular
		if(latitudeMax > ThroughputMeasure.LATITUDE_MAX)
		{
			double diff = latitudeMax - ThroughputMeasure.LATITUDE_MAX;
			latitudeMax = ThroughputMeasure.LATITUDE_MIN + diff;
		}
		else
		{
			
		}
		double longitudeMin = ThroughputMeasure.LONGITUDE_MIN 
				+queryRand.nextDouble()*(ThroughputMeasure.LONGITUDE_MAX - ThroughputMeasure.LONGITUDE_MIN);
		
		predLength = (queryRand.nextDouble()*(ThroughputMeasure.LONGITUDE_MAX - ThroughputMeasure.LONGITUDE_MIN));
		double longitudeMax = longitudeMin + predLength;
		
		if( longitudeMax > ThroughputMeasure.LONGITUDE_MAX )
		{
			double diff = longitudeMax - ThroughputMeasure.LONGITUDE_MAX;
			longitudeMax = ThroughputMeasure.LONGITUDE_MIN + diff;
		}
		else
		{
			
		}
//		latitudeMin = ThroughputMeasure.LATITUDE_MIN;
//		latitudeMax = ThroughputMeasure.LATITUDE_MAX;
//		longitudeMin = ThroughputMeasure.LONGITUDE_MIN;
//		longitudeMax = ThroughputMeasure.LONGITUDE_MAX;
		
    	String mysqlQuery = "SELECT nodeGUID from "+tableName+" WHERE ( ";
    	
    	String latName = ThroughputMeasure.latitudeAttrName;
    	// normal case of lower value being lesser than the upper value
		if( latitudeMin <= latitudeMax )
		{
			String queryMin  = latitudeMin+"";
			String queryMax  = latitudeMax+"";
			
			mysqlQuery = mysqlQuery + " ( "+latName +" >= "+queryMin +" AND " 
						+latName +" <= "+queryMax+" ) AND ";
		}
		else
		{
			String queryMin  = ThroughputMeasure.LATITUDE_MIN+"";
			String queryMax  = latitudeMax+"";
			
			mysqlQuery = mysqlQuery + " ( "
					+" ( "+latName +" >= "+queryMin +" AND " 
					+latName +" <= "+queryMax+" ) OR ";
			
			queryMin  = latitudeMin+"";
			queryMax  = ThroughputMeasure.LATITUDE_MAX+"";
			
			mysqlQuery = mysqlQuery +" ( "+latName +" >= "+queryMin +" AND " 
					+latName +" <= "+queryMax+" ) ) AND ";	
		}
		
		
		String longName = ThroughputMeasure.longitudeAttrName;
		if( longitudeMin <= longitudeMax )
		{
			String queryMin  = longitudeMin+"";
			String queryMax  = longitudeMax+"";
			
			// it is assumed that the strings in query(pqc.getLowerBound() or pqc.getUpperBound()) 
			// will have single or double quotes in them so we don't need to them separately in mysql query
			mysqlQuery = mysqlQuery + " ( "+longName +" >= "+queryMin +" AND " 
						+longName +" <= "+queryMax+" ) )";
		}
		else
		{
			String queryMin  = ThroughputMeasure.LONGITUDE_MIN+"";
			String queryMax  = longitudeMax+"";
				
			mysqlQuery = mysqlQuery + " ( "
						+" ( "+longName +" >= "+queryMin +" AND " 
						+longName +" <= "+queryMax+" ) OR ";
						
			queryMin  = longitudeMin+"";
			queryMax  = ThroughputMeasure.LONGITUDE_MAX+"";
				
			mysqlQuery = mysqlQuery +" ( "+longName +" >= "+queryMin +" AND " 
						+longName +" <= "+queryMax+" ) ) )";
		}
		return mysqlQuery;
	}
	
	private boolean findOverlapLatitude(double latitudeMin, double latitudeMax)
	{	
		if( (latitudeMin >= ThroughputMeasure.LATITUDE_MIN_NODE0 && 
				latitudeMin <= ThroughputMeasure.LATITUDE_MAX_NODE0) ||
				(latitudeMax >= ThroughputMeasure.LATITUDE_MIN_NODE0 && 
				latitudeMax <= ThroughputMeasure.LATITUDE_MAX_NODE0) ||
				(ThroughputMeasure.LATITUDE_MIN_NODE0 >= latitudeMin && 
				ThroughputMeasure.LATITUDE_MAX_NODE0 <= latitudeMax) )
		{
			return true;
		}
		return false;
	}
	
	private boolean findOverlapLongitude(double longitudeMin, double longitudeMax)
	{
		if( (longitudeMin >= ThroughputMeasure.LONGITUDE_MIN_NODE0 && 
				longitudeMin <= ThroughputMeasure.LONGITUDE_MAX_NODE0) ||
				(longitudeMax >= ThroughputMeasure.LONGITUDE_MIN_NODE0 && 
				longitudeMax <= ThroughputMeasure.LONGITUDE_MAX_NODE0) ||
				(ThroughputMeasure.LONGITUDE_MIN_NODE0 >= longitudeMin && 
				ThroughputMeasure.LONGITUDE_MAX_NODE0 <= longitudeMax) )
		{
			return true;
		}
		return false;
	}*/
}