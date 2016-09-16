package edu.umass.cs.hyperdexExperiments;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;


public class UserInitializationClass extends AbstractRequestSendingClass
{
	// different random generator for each variable, as using one for all of them
	// doesn't give uniform properties.
	private final Random initRand;
	
	public UserInitializationClass()
	{
		super( SearchAndUpdateDriver.INSERT_LOSS_TOLERANCE );
		initRand = new Random(SearchAndUpdateDriver.myID*100);
	}
	
	private void sendAInitMessage(int guidNum) throws Exception
	{
		//GuidEntry userGuidEntry = null;
//		if( SearchAndUpdateDriver.useGNS )
//		{
//			userGuidEntry = SearchAndUpdateDriver.gnsClient.guidCreate(
//					SearchAndUpdateDriver.accountGuid, SearchAndUpdateDriver.guidPrefix+guidNum);
//		}
		Map<String, Object> attrValMap = new HashMap<String, Object>();
		
		//JSONObject attrValJSON = new JSONObject();
		
		double attrDiff   = SearchAndUpdateDriver.ATTR_MAX-SearchAndUpdateDriver.ATTR_MIN;
		
		for( int i=0;i<SearchAndUpdateDriver.numAttrs;i++ )
		{
			String attrName = SearchAndUpdateDriver.attrPrefix+i;
			double attrVal  = SearchAndUpdateDriver.ATTR_MIN 
					+ attrDiff * initRand.nextDouble();
			//attrValJSON.put(attrName, attrVal);
			attrValMap.put(attrName, attrVal);
		}
		
		String userGUID = "";
		userGUID = SearchAndUpdateDriver.getSHA1(SearchAndUpdateDriver.guidPrefix+guidNum);
		
//		ExperimentUpdateReply updateRep = new ExperimentUpdateReply(guidNum, userGUID);
//		
//		SearchAndUpdateDriver.csClient.sendUpdateWithCallBack
//										(userGUID, null, 
//										attrValJSON, -1, updateRep, this.getCallBack());		
		
		UpdateTask updTask = new UpdateTask(attrValMap, userGUID, this);
		SearchAndUpdateDriver.taskES.execute(updTask);
	}
	
	
	public void initializaRateControlledRequestSender() throws Exception
	{
//		if(SearchAndUpdateDriver.useGNS)
//		{
//			SearchAndUpdateDriver.accountGuid = SearchAndUpdateDriver.gnsClient.accountGuidCreate("gnsumass@gmail.com", "testPass");
//			Thread.sleep(5000);
//			System.out.println("account guid created "+SearchAndUpdateDriver.accountGuid.getGuid());
//		}
		
		this.startExpTime();
		double reqspms = SearchAndUpdateDriver.initRate/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double totalNumUsersSent = 0;
		
		while(  totalNumUsersSent < SearchAndUpdateDriver.numUsers  )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				sendAInitMessage((int)totalNumUsersSent);
				totalNumUsersSent++;
				numSent++;
				assert(numSent == totalNumUsersSent);
				if(totalNumUsersSent >= SearchAndUpdateDriver.numUsers)
				{
					break;
				}
			}
			if(totalNumUsersSent >= SearchAndUpdateDriver.numUsers)
			{
				break;
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
				sendAInitMessage((int)totalNumUsersSent);
				totalNumUsersSent++;
				numSent++;
				assert(numSent == totalNumUsersSent);
				if(totalNumUsersSent >= SearchAndUpdateDriver.numUsers)
				{
					break;
				}
			}
			if(totalNumUsersSent >= SearchAndUpdateDriver.numUsers)
			{
				break;
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("UserInit eventual sending rate "+sendingRate);
		
		waitForFinish();
		//Thread.sleep(WAIT_TIME);
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("UserInit result:Goodput "+sysThrput);	
		//System.exit(0);
	}
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			System.out.println("UserInit reply recvd "+userGUID+" time taken "+timeTaken+
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