package edu.umass.cs.mysqlBenchmarking;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;


public class BulkInitializeClass extends AbstractRequestSendingClass
{
	private Random updateRand;
	public BulkInitializeClass()
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
		} 
		catch (Exception e) 
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
		
		long currUserGuidNum   = 0;
		
		List<UpdateInfoClass> udpateList = new LinkedList<UpdateInfoClass>();
		
		
		while( ( currUserGuidNum < MySQLThroughputBenchmarking.numGuids ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				UpdateInfoClass updateInfo = getAUpdateObject(currUserGuidNum);
				udpateList.add(updateInfo);
				if(udpateList.size() == MySQLThroughputBenchmarking.batchSize)
				{
					doUpdate(udpateList);
					// not clearing but creating a new list to remove any contention 
					// from the thread that actually perform update
					udpateList = new LinkedList<UpdateInfoClass>();
				}
				
				currUserGuidNum++;
				if(currUserGuidNum >= MySQLThroughputBenchmarking.numGuids)
					break;
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
				UpdateInfoClass updateInfo = getAUpdateObject(currUserGuidNum);
				udpateList.add(updateInfo);
				if(udpateList.size() == MySQLThroughputBenchmarking.batchSize)
				{
					doUpdate(udpateList);
					// not clearing but creating a new list to remove any contention 
					// from the thread that actually perform update
					udpateList = new LinkedList<UpdateInfoClass>();
				}
				
				currUserGuidNum++;
				if(currUserGuidNum >= MySQLThroughputBenchmarking.numGuids)
					break;
				//numSent++;
			}
			
			if( udpateList.size() > 0 )
			{
				doUpdate(udpateList);
				// not clearing but creating a new list to remove any contention 
				// from the thread that actually perform update
				udpateList = new LinkedList<UpdateInfoClass>();
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
	
	
	private UpdateInfoClass getAUpdateObject(long currUserGuidNum)
	{
		HashMap<String, String> attrValMap = new HashMap<String, String>();
		
		String guid = MySQLThroughputBenchmarking.getSHA1
				(MySQLThroughputBenchmarking.guidPrefix+currUserGuidNum);
		
		Iterator<String> atterIter = MySQLThroughputBenchmarking.attrMap.keySet().iterator();
		
		while( atterIter.hasNext() )
		{
			String attrName = atterIter.next();
			attrValMap.put(attrName, 
					MySQLThroughputBenchmarking.ATTR_MAX * updateRand.nextDouble()+"");
		}
		UpdateInfoClass updateInfo = new UpdateInfoClass(guid, attrValMap);
		return updateInfo;
	}
	
	
	private void doUpdate(List<UpdateInfoClass> updateInfo)
	{
		numSent++;
		
		BulkInitializeTask updTask = new BulkInitializeTask(updateInfo, this);
		MySQLThroughputBenchmarking.taskES.execute(updTask);
	}
	
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			System.out.println("Init reply recvd "+userGUID+" time taken "+timeTaken+
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
	}
}