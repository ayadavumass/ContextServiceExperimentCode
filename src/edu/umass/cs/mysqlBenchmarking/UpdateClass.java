package edu.umass.cs.mysqlBenchmarking;


import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;
/**
 * Updates locations of all users after every 
 * granularityOfGeolocationUpdate
 * @author adipc
 */
public class UpdateClass extends AbstractRequestSendingClass implements Runnable
{
	private Random updateRand;
	public UpdateClass()
	{
		super(MySQLThroughputBenchmarking.UPD_LOSS_TOLERANCE);
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
		double reqspms = MySQLThroughputBenchmarking.updateRequestsps/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double currUserGuidNum   = 0;
		
		//while( ( totalNumUsersSent < numUsers ) )
		while( ( (System.currentTimeMillis() - expStartTime) 
						< MySQLThroughputBenchmarking.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				doUpdate((int)currUserGuidNum);
				currUserGuidNum++;
				currUserGuidNum=((int)currUserGuidNum)%MySQLThroughputBenchmarking.numGuids;
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
				currUserGuidNum=((int)currUserGuidNum)%MySQLThroughputBenchmarking.numGuids;
				//numSent++;
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Update eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Update result:Goodput "+sysThrput);
	}
	
	
	private void doUpdate(int currUserGuidNum) throws JSONException
	{
		numSent++;
		String guid = MySQLThroughputBenchmarking.getSHA1
				(MySQLThroughputBenchmarking.guidPrefix+currUserGuidNum);
		String attrName = "attr"+updateRand.nextInt(MySQLThroughputBenchmarking.numAttrs);
		double value = 1500*+updateRand.nextDouble();
		JSONObject updateJSON = new JSONObject();
		
		updateJSON.put(attrName, value);
		
		UpdateTask updTask = new UpdateTask( guid, updateJSON, this);
		MySQLThroughputBenchmarking.taskES.execute(updTask);
	}

	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			System.out.println("Update reply recvd "+userGUID+" time taken "+timeTaken+
					" numSent "+numSent+" numRecvd "+numRecvd);
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
		// TODO Auto-generated method stub
	}
}