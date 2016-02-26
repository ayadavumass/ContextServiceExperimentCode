package edu.umass.cs.genericExpClient;

import java.util.Random;
import org.json.JSONException;
import org.json.JSONObject;


public class UpdateFixedUsers extends AbstractRequestSendingClass implements Runnable
{	
	private final Random updateRand;
	
	public UpdateFixedUsers()
	{
		super(SearchAndUpdateDriver.UPD_LOSS_TOLERANCE);
		updateRand = new Random(SearchAndUpdateDriver.myID*200);
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
		double reqspms = SearchAndUpdateDriver.updateRate/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double currUserGuidNum   = 0;
		
		//while( ( totalNumUsersSent < numUsers ) )
		while( ( (System.currentTimeMillis() - expStartTime) < SearchAndUpdateDriver.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				sendALocMessage((int)currUserGuidNum);
				currUserGuidNum++;
				currUserGuidNum=((int)currUserGuidNum)%SearchAndUpdateDriver.numUsers;
				numSent++;
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
				sendALocMessage((int)currUserGuidNum);
				currUserGuidNum++;
				currUserGuidNum=((int)currUserGuidNum)%SearchAndUpdateDriver.numUsers;
				numSent++;
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("LocationUpd eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("LocationUpd result:Goodput "+sysThrput);
	}
	
	private void sendALocMessage(int currUserGuidNum)
	{
//		UserRecordInfo currUserInfo = SearchAndUpdateDriver.userInfoHashMap.get
//				(SearchAndUpdateDriver.guidPrefix+currUserGuidNum);
		
		String userGUID = "";
		if( SearchAndUpdateDriver.useGNS )
		{
//			userGUID = userGuidEntry.getGuid();
		}
		else
		{
			userGUID = SearchAndUpdateDriver.getSHA1(SearchAndUpdateDriver.guidPrefix+currUserGuidNum);
		}
		
		int randomAttrNum = updateRand.nextInt(SearchAndUpdateDriver.numAttrs);
		double randVal = SearchAndUpdateDriver.ATTR_MIN 
				+updateRand.nextDouble()*(SearchAndUpdateDriver.ATTR_MAX - SearchAndUpdateDriver.ATTR_MIN);
		
		JSONObject attrValJSON = new JSONObject();
		try
		{			
			attrValJSON.put(SearchAndUpdateDriver.attrPrefix+randomAttrNum, randVal);
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		//System.out.println("Updating "+currUserGuidNum+" "+attrValJSON);
		UpdateTask updTask = new UpdateTask( attrValJSON, userGUID, this );
		SearchAndUpdateDriver.taskES.execute(updTask);
	}
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			System.out.println("LocUpd reply recvd "+userGUID+" time taken "+timeTaken+
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