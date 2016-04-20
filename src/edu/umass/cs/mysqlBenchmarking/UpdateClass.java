package edu.umass.cs.mysqlBenchmarking;


import java.util.Random;
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
		super(MySQLBenchmarking.UPD_LOSS_TOLERANCE);
		updateRand = new Random();
	}
	
	@Override
	public void run()
	{
		try 
		{
			this.startExpTime();
			locUpdRateControlledRequestSender();
		} catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	private void locUpdRateControlledRequestSender() throws Exception
	{
		double reqspms = MySQLBenchmarking.updateRequestsps/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double currUserGuidNum   = 0;
		
		//while( ( totalNumUsersSent < numUsers ) )
		while( ( (System.currentTimeMillis() - expStartTime) < MySQLBenchmarking.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				doUpdate((int)currUserGuidNum);
				currUserGuidNum++;
				currUserGuidNum=((int)currUserGuidNum)%MySQLBenchmarking.numGuids;
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
				currUserGuidNum=((int)currUserGuidNum)%MySQLBenchmarking.numGuids;
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
	
	
	private void doUpdate(int currUserGuidNum)
	{
		numSent++;
		String guid = MySQLBenchmarking.getSHA1(MySQLBenchmarking.guidPrefix+currUserGuidNum);
		double value1= 1+updateRand.nextInt(1499);
		double value2 =  1+updateRand.nextInt(1499);

		UpdateTask updTask = new UpdateTask( guid, value1, value2, this);
		MySQLBenchmarking.taskES.execute(updTask);
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