package edu.umass.cs.mysqlBenchmarking;

import java.util.Random;

public class GetClass extends AbstractRequestSendingClass
{
	private Random getRand;
	public GetClass()
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
		double reqspms = MySQLThroughputBenchmarking.requestsps/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		int currUserGuidNum;
		
		//while( ( currUserGuidNum < MySQLThroughputBenchmarking.numGuids ) )
		while( ( (System.currentTimeMillis() - expStartTime) 
				< MySQLThroughputBenchmarking.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				currUserGuidNum = getRand.nextInt(MySQLThroughputBenchmarking.numGuids);
				doGet(currUserGuidNum);
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
				currUserGuidNum = getRand.nextInt(MySQLThroughputBenchmarking.numGuids);
				doGet(currUserGuidNum);
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Get eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Get result:Goodput "+sysThrput);
	}
	
	private void doGet(int currUserGuidNum)
	{
		numSent++;
		String guid = MySQLThroughputBenchmarking.getSHA1
							(MySQLThroughputBenchmarking.guidPrefix+currUserGuidNum);
		
		GetTask getTask = new GetTask( guid, this);
		MySQLThroughputBenchmarking.taskES.execute(getTask);
	}
	
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized( waitLock )
		{
			numRecvd++;
			System.out.println("Get reply recvd "+userGUID+" time taken "+timeTaken+
					" numSent "+numSent+" numRecvd "+numRecvd);
			//if(currNumReplyRecvd == currNumReqSent)
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