package edu.umass.cs.mysqlLocationBechmarking;

import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

public class InitializeClass extends AbstractRequestSendingClass
{
	private Random updateRand;
	public InitializeClass()
	{
		super(MySQLThroughputBenchmarking.INSERT_LOSS_TOLERANCE);
		updateRand = new Random();
	}
	
	@Override
	public void run()
	{
		try 
		{
			this.startExpTime();
			updRateControlledRequestSender();
		} catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	private void updRateControlledRequestSender() throws Exception
	{
		double reqspms = 100.0/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double currUserGuidNum   = 0;
		
		while( ( currUserGuidNum < MySQLThroughputBenchmarking.numGuids ) )
		//while( ( (System.currentTimeMillis() - expStartTime) < MySQLBenchmarking.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				doUpdate((int)currUserGuidNum);
				currUserGuidNum++;
				if(currUserGuidNum >= MySQLThroughputBenchmarking.numGuids)
					break;
				//numSent++;
			}
			if(currUserGuidNum >= MySQLThroughputBenchmarking.numGuids)
				break;
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
				doUpdate((int)currUserGuidNum);
				currUserGuidNum++;
				if(currUserGuidNum >= MySQLThroughputBenchmarking.numGuids)
					break;
				//numSent++;
			}
			if(currUserGuidNum >= MySQLThroughputBenchmarking.numGuids)
				break;
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Init eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Init result:Goodput "+sysThrput);
	}
	
	
	private void doUpdate(int currUserGuidNum)
	{
		numSent++;
		String guid = MySQLThroughputBenchmarking.getSHA1
							(MySQLThroughputBenchmarking.guidPrefix+currUserGuidNum);
		
		JSONObject attrValJSON = new JSONObject();
		
		double latVal  = MySQLThroughputBenchmarking.LAT_MIN 
			+ updateRand.nextDouble() * ( MySQLThroughputBenchmarking.LAT_MAX 
					- MySQLThroughputBenchmarking.LAT_MIN );
		
		double longVal = MySQLThroughputBenchmarking.LONG_MIN + 
				updateRand.nextDouble() * ( MySQLThroughputBenchmarking.LONG_MAX 
						- MySQLThroughputBenchmarking.LONG_MIN );
		
		try 
		{
			attrValJSON.put("latitude", latVal);
			attrValJSON.put("longitude", longVal);
		} 
		catch (JSONException e) 
		{
			e.printStackTrace();
		}

		InitializeTask updTask = new InitializeTask( guid, attrValJSON, this);
		MySQLThroughputBenchmarking.taskES.execute(updTask);
	}

	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
//			System.out.println("Init reply recvd "+userGUID+" time taken "+timeTaken+
//					" numSent "+numSent+" numRecvd "+numRecvd);
			//if(currNumReplyRecvd == currNumReqSent)
			if(checkForCompletionWithLossTolerance(numSent, numRecvd))
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