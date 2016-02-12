package edu.umass.cs.weatherExpClient;

import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;


public class LocationUpdateFixedUsers extends AbstractRequestSendingClass implements Runnable
{
	
	private final Random updateRand;
	public LocationUpdateFixedUsers()
	{
		super(WeatherAndMobilityBoth.UPD_LOSS_TOLERANCE);
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
		//double reqspms = (WeatherAndMobilityBoth.numUsers * 1.0)/WeatherAndMobilityBoth.granularityOfGeolocationUpdate;
		double reqspms = WeatherAndMobilityBoth.updateRate/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double currUserGuidNum   = 0;
		
		//while( ( totalNumUsersSent < numUsers ) )
		while( ( (System.currentTimeMillis() - expStartTime) < WeatherAndMobilityBoth.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				sendALocMessage((int)currUserGuidNum);
				currUserGuidNum++;
				currUserGuidNum=((int)currUserGuidNum)%WeatherAndMobilityBoth.numUsers;
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
				currUserGuidNum=((int)currUserGuidNum)%WeatherAndMobilityBoth.numUsers;
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
		UserRecordInfo currUserInfo = WeatherAndMobilityBoth.userInfoHashMap.get
				(WeatherAndMobilityBoth.guidPrefix+currUserGuidNum);
		
		JSONObject attrValJSON = new JSONObject();
		try
		{
			double newLat = WeatherAndMobilityBoth.LATITUDE_MIN 
					+updateRand.nextDouble()*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
			
			double newLong = WeatherAndMobilityBoth.LONGITUDE_MIN 
					+updateRand.nextDouble()*(WeatherAndMobilityBoth.LONGITUDE_MAX - WeatherAndMobilityBoth.LONGITUDE_MIN);
			
			attrValJSON.put(WeatherAndMobilityBoth.latitudeAttrName, newLat);
			attrValJSON.put(WeatherAndMobilityBoth.longitudeAttrName, newLong);
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		//System.out.println("Updating "+currUserGuidNum+" "+attrValJSON);
		UpdateTask updTask = new UpdateTask( attrValJSON, currUserInfo, this );
		WeatherAndMobilityBoth.taskES.execute(updTask);
		
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
		// TODO Auto-generated method stub
	}
}