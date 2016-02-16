package edu.umass.cs.modelParameters;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;
import java.util.Vector;

public class OverlapClass extends AbstractRequestSendingClass implements Runnable
{
	private final Random queryRand;
	private final Vector<String> subspaceQueries;
	
	public OverlapClass()
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
		double reqspms = ThroughputMeasure.overlapRequestsps/1000.0;
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
		OverlapTask overlapTask = new OverlapTask( mysqlQuery, this );
		ThroughputMeasure.taskES.execute(overlapTask);
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
			System.out.println("Overlap reply recvd size "+resultSize+" time taken "+timeTaken+
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
			String searchQueryFileName 
						= "serv"+ThroughputMeasure.nodeId+"OverlapQueries.txt";
			
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
}