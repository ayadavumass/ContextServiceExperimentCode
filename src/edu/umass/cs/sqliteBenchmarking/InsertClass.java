package edu.umass.cs.sqliteBenchmarking;

import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

public class InsertClass extends AbstractRequestSendingClass
{
	private Random updateRand;
	private double sumUpdTime = 0;
	
	public InsertClass()
	{
		super(SQLiteThroughputBenchmarking.INSERT_LOSS_TOLERANCE);
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
		double reqspms = SQLiteThroughputBenchmarking.requestsps/1000.0;
		long currTime  = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double currUserGuidNum   = 1000000;
		int numGuidsInserted = 0;
		while( ( numGuidsInserted < SQLiteThroughputBenchmarking.numGuidsToInsert ) )
//		while( ( (System.currentTimeMillis() - expStartTime)
//				< MySQLThroughputBenchmarking.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				doUpdate((int)currUserGuidNum);
				currUserGuidNum++;
				numGuidsInserted++;
				if(numGuidsInserted >= SQLiteThroughputBenchmarking.numGuidsToInsert)
					break;
				//numSent++;
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
				doUpdate((int)currUserGuidNum);
				currUserGuidNum++;
				numGuidsInserted++;
				if(numGuidsInserted >= SQLiteThroughputBenchmarking.numGuidsToInsert)
					break;
				//numSent++;
			}			
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Insert eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Insert result:Goodput "+sysThrput);
	}
	
	private void doUpdate(int currUserGuidNum)
	{
		numSent++;
		String guid = SQLiteThroughputBenchmarking.getSHA1
							(SQLiteThroughputBenchmarking.guidPrefix+currUserGuidNum);
		
		JSONObject attrValJSON = new JSONObject();
		
		for(int i=0; i<SQLiteThroughputBenchmarking.numAttrs; i++)
		{
			try 
			{
				attrValJSON.put("attr"+i, 1500 * updateRand.nextDouble());
			} 
			catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		
		InsertTask updTask = new InsertTask( guid, attrValJSON, this);
		SQLiteThroughputBenchmarking.taskES.execute(updTask);
	}
	
	public double getAvgInsertTime()
	{
		return this.sumUpdTime/numRecvd;
	}

	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			sumUpdTime = sumUpdTime + timeTaken;
//			System.out.println("Insert reply recvd "+userGUID+" time taken "+timeTaken+
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