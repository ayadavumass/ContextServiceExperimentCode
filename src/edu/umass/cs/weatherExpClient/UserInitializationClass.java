package edu.umass.cs.weatherExpClient;

import java.util.Random;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GuidEntry;

public class UserInitializationClass extends AbstractRequestSendingClass
{
	private final Random angleRand;
	private final Random latRand;
	private final Random longRand;
	
	public UserInitializationClass()
	{
		super(WeatherAndMobilityBoth.INSERT_LOSS_TOLERANCE);
		angleRand = new Random(WeatherAndMobilityBoth.myID);
		latRand = new Random(WeatherAndMobilityBoth.myID);
		longRand = new Random(WeatherAndMobilityBoth.myID);
	}
	
	private void sendAInitMessage(int guidNum) throws Exception
	{
		GuidEntry userGuidEntry = null;
		if( WeatherAndMobilityBoth.useGNS )
		{
			userGuidEntry = WeatherAndMobilityBoth.gnsClient.guidCreate(
					WeatherAndMobilityBoth.accountGuid, WeatherAndMobilityBoth.guidPrefix+guidNum);
		}
		
		double latDiff   = WeatherAndMobilityBoth.LATITUDE_MAX-WeatherAndMobilityBoth.LATITUDE_MIN;
		double longDiff  = WeatherAndMobilityBoth.LONGITUDE_MAX-WeatherAndMobilityBoth.LONGITUDE_MIN;
		
		double userLat   = WeatherAndMobilityBoth.LATITUDE_MIN + latDiff * latRand.nextDouble();
		double userLong  = WeatherAndMobilityBoth.LONGITUDE_MIN + longDiff * longRand.nextDouble();
		
		int userState=-1;
		
		if( (guidNum%2) == 0 )
		{
			userState = WeatherAndMobilityBoth.STATE_DRIVING;
		}
		else if( (guidNum%2) == 1 )
		{
			userState = WeatherAndMobilityBoth.STATE_WALKING;
		}
//		else if( (guidNum%3) == 2 )
//		{
//			userState = WeatherAndMobilityBoth.STATE_STATIONARY;
//		}
		
		JSONObject attrValJSON = null;
		
		if( WeatherAndMobilityBoth.useContextService )
		{
			attrValJSON = WeatherAndMobilityBoth.getUpdateJSONForCS(userState, userLat, userLong);
		}
		else
		{
			attrValJSON = WeatherAndMobilityBoth.getUpdateJSONForGNS(userState, userLat, userLong);
		}
		
		// angle is between 0 to 360
		// it is set once for each state of user activity
		double angleOfMovement = angleRand.nextDouble()*360;
		String userGUID = "";
		if( WeatherAndMobilityBoth.useGNS )
		{
			userGUID = userGuidEntry.getGuid();
		}
		else
		{
			userGUID = WeatherAndMobilityBoth.getSHA1(WeatherAndMobilityBoth.guidPrefix+guidNum);
		}
		
		UserRecordInfo userRecordInfo 
			= new UserRecordInfo(WeatherAndMobilityBoth.guidPrefix+guidNum, userGuidEntry, userGUID);
		userRecordInfo.setGeoLocation(userLat, userLong);
		userRecordInfo.setUserActivity(userState);
		userRecordInfo.setAngleOfMovement(angleOfMovement);
		
		WeatherAndMobilityBoth.userInfoHashMap.put(WeatherAndMobilityBoth.guidPrefix+guidNum, userRecordInfo);
		
		//System.out.println("Initializing "+userGUID+" "+attrValJSON);
		
		UpdateTask updTask = new UpdateTask(attrValJSON, userRecordInfo, this);
		WeatherAndMobilityBoth.taskES.execute(updTask);
	}
	
	public void initializaRateControlledRequestSender() throws Exception
	{
		if(WeatherAndMobilityBoth.useGNS)
		{
			WeatherAndMobilityBoth.accountGuid = WeatherAndMobilityBoth.gnsClient.accountGuidCreate("gnsumass@gmail.com", "testPass");
			Thread.sleep(5000);
			System.out.println("account guid created "+WeatherAndMobilityBoth.accountGuid.getGuid());
		}
		
		this.startExpTime();
		double reqspms = (WeatherAndMobilityBoth.numUsers * 1.0)/WeatherAndMobilityBoth.granularityOfInitialization;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double totalNumUsersSent = 0;
		
		while(  totalNumUsersSent < WeatherAndMobilityBoth.numUsers  )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				sendAInitMessage((int)totalNumUsersSent);
				totalNumUsersSent++;
				numSent++;
				assert(numSent == totalNumUsersSent);
				if(totalNumUsersSent >= WeatherAndMobilityBoth.numUsers)
				{
					break;
				}
			}
			if(totalNumUsersSent >= WeatherAndMobilityBoth.numUsers)
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
				if(totalNumUsersSent >= WeatherAndMobilityBoth.numUsers)
				{
					break;
				}
			}
			if(totalNumUsersSent >= WeatherAndMobilityBoth.numUsers)
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