package edu.umass.cs.sqliteBenchmarking;

import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

public class DeleteClass extends AbstractRequestSendingClass
{
	private Random updateRand;
	
	private double sumDelTime;
	
	public DeleteClass()
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
			deleteRateControlledRequestSender();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
//	private void deleteRateControlledRequestSender() throws Exception
//	{
//		double reqspms = MySQLThroughputBenchmarking.requestsps/1000.0;
//		long currTime  = 0;
//		
//		// sleep for 100ms
//		double numberShouldBeSentPerSleep = reqspms*100.0;
//		
//		double currUserGuidNum   = 0;
//		
//		//while( ( currUserGuidNum < MySQLThroughputBenchmarking.numGuids ) )
//		while( ( (System.currentTimeMillis() - expStartTime)
//				< MySQLThroughputBenchmarking.EXPERIMENT_TIME ) )
//		{
//			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
//			{
//				doUpdate((int)currUserGuidNum);
//				currUserGuidNum++;
//				//numSent++;
//			}
//			
//			currTime = System.currentTimeMillis();
//			
//			double timeElapsed = ((currTime- expStartTime)*1.0);
//			double numberShouldBeSentByNow = timeElapsed*reqspms;
//			double needsToBeSentBeforeSleep = numberShouldBeSentByNow - numSent;
//			if(needsToBeSentBeforeSleep > 0)
//			{
//				needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
//			}
//			
//			for(int i=0;i<needsToBeSentBeforeSleep;i++)
//			{
//				doUpdate((int)currUserGuidNum);
//				currUserGuidNum++;
//				//numSent++;
//			}			
//			Thread.sleep(100);
//		}
//		
//		long endTime = System.currentTimeMillis();
//		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
//		double sendingRate = (numSent * 1.0)/(timeInSec);
//		System.out.println("Insert eventual sending rate "+sendingRate);
//		
//		waitForFinish();
//		double endTimeReplyRecvd = System.currentTimeMillis();
//		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
//		
//		System.out.println("Insert result:Goodput "+sysThrput);
//	}
	
	private void deleteRateControlledRequestSender() throws Exception
	{
		double reqspms = SQLiteThroughputBenchmarking.requestsps/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double currUserGuidNum   = 0;
		
		while( ( currUserGuidNum < SQLiteThroughputBenchmarking.numGuids ) )
		//while( ( (System.currentTimeMillis() - expStartTime) < MySQLBenchmarking.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				doDelete((int)currUserGuidNum);
				currUserGuidNum++;
				if(currUserGuidNum >= SQLiteThroughputBenchmarking.numGuids)
					break;
				//numSent++;
			}
			if(currUserGuidNum >= SQLiteThroughputBenchmarking.numGuids)
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
				doDelete((int)currUserGuidNum);
				currUserGuidNum++;
				if(currUserGuidNum >= SQLiteThroughputBenchmarking.numGuids)
					break;
				//numSent++;
			}
			if(currUserGuidNum >= SQLiteThroughputBenchmarking.numGuids)
				break;
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Delete eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Delete result:Goodput "+sysThrput);
	}
	
	private void doDelete(int currUserGuidNum)
	{
		numSent++;
		String guid = SQLiteThroughputBenchmarking.getSHA1
							(SQLiteThroughputBenchmarking.guidPrefix+currUserGuidNum);
		
		DeleteTask delTask = new DeleteTask( guid, this);
		SQLiteThroughputBenchmarking.taskES.execute(delTask);
	}
	
	public double getAvgDelTime()
	{
		return this.sumDelTime/numRecvd;
	}
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			this.sumDelTime = sumDelTime+timeTaken;
			
//			System.out.println("Delete reply recvd "+userGUID+" time taken "+timeTaken+
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