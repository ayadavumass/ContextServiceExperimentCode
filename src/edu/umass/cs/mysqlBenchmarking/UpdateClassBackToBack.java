package edu.umass.cs.mysqlBenchmarking;


import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;
/**
 * Updates locations of all users after every 
 * granularityOfGeolocationUpdate
 * @author adipc
 */
public class UpdateClassBackToBack extends AbstractRequestSendingClass
{
	private Random updateRand;
	public UpdateClassBackToBack()
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
		long numOfBackToBackReqToSend = (long)MySQLThroughputBenchmarking.requestsps;
		
		long numReqsSent = 0;
		
		int currUserGuidNum;
		
		long start = System.currentTimeMillis();
		while( numReqsSent < numOfBackToBackReqToSend )
		{
			currUserGuidNum = updateRand.nextInt(MySQLThroughputBenchmarking.numGuids);
			doUpdate((int)currUserGuidNum);
			numReqsSent++;
		}
		long end = System.currentTimeMillis();
		double timePerReq = ((end-start)*1.0)/numOfBackToBackReqToSend;
		
		System.out.println("Time for back-to-back update request rate "+timePerReq);
		
		threadFinished = true;
		synchronized( threadFinishLock )
		{
			threadFinishLock.notify();
		}
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
//		synchronized(waitLock)
//		{
//			numRecvd++;
//			System.out.println("Update reply recvd "+userGUID+" time taken "+timeTaken+
//					" numSent "+numSent+" numRecvd "+numRecvd);
//			//if(currNumReplyRecvd == currNumReqSent)
//			if(checkForCompletionWithLossTolerance(numSent, numRecvd))
//			{
//				waitLock.notify();
//			}
//		}
	}
	
	@Override
	public void incrementSearchNumRecvd(int resultSize, long timeTaken) 
	{
	}
}