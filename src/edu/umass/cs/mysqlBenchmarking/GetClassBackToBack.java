package edu.umass.cs.mysqlBenchmarking;

import java.util.Random;

public class GetClassBackToBack extends AbstractRequestSendingClass
{
	private Random getRand;
	public GetClassBackToBack()
	{
		super(MySQLThroughputBenchmarking.INSERT_LOSS_TOLERANCE);
		getRand = new Random();
	}
	
	@Override
	public void run()
	{
		try
		{
			this.startExpTime();
			getRateControlledRequestSender();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
		}
	}
	
	private void getRateControlledRequestSender() throws Exception
	{
		long numOfBackToBackReqToSend = (long)MySQLThroughputBenchmarking.requestsps;
		
		long numReqsSent = 0;
		
		int currUserGuidNum;
		
		long start = System.currentTimeMillis();
		while( numReqsSent < numOfBackToBackReqToSend )
		{
			currUserGuidNum = getRand.nextInt((int)MySQLThroughputBenchmarking.numGuids);
			doGet(currUserGuidNum);
			numReqsSent++;
		}
		long end = System.currentTimeMillis();
		double timePerReq = ((end-start)*1.0)/numOfBackToBackReqToSend;
		
		System.out.println("Time for back-to-back get request rate "+timePerReq);
		
		threadFinished = true;
		synchronized( threadFinishLock )
		{
			threadFinishLock.notify();
		}
	}
	
	private void doGet(int currUserGuidNum)
	{
		numSent++;
		String guid = MySQLThroughputBenchmarking.getSHA1
							(MySQLThroughputBenchmarking.guidPrefix+currUserGuidNum);
		
		GetTask getTask = new GetTask( guid, this);
		getTask.run();
		//MySQLThroughputBenchmarking.taskES.execute(getTask);
	}
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
//		synchronized( waitLock )
//		{
//			numRecvd++;
//			System.out.println("Get reply recvd "+userGUID+" time taken "+timeTaken+
//					" numSent "+numSent+" numRecvd "+numRecvd);
//			//if(currNumReplyRecvd == currNumReqSent)
//			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
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