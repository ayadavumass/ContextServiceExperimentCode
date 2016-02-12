package edu.umass.cs.weatherExpClient;

import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

public class StateChangeClass extends AbstractRequestSendingClass implements Runnable
{
	private Random stateChangeRand;
	
	public StateChangeClass()
	{
		super(WeatherAndMobilityBoth.UPD_LOSS_TOLERANCE);
		stateChangeRand = new Random();
	}
	
	@Override
	public void run()
	{
		this.startExpTime();
		try
		{
			startChangeRateControlledRequestSender();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		catch(Error ex)
		{
			ex.printStackTrace();
		}
	}
	
	private void startChangeRateControlledRequestSender() throws Exception
	{
		double reqspms = (WeatherAndMobilityBoth.numUsers * 1.0)/WeatherAndMobilityBoth.granularityOfStateChange;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double currUserGuidNum   = 0;
		
		//while( ( totalNumUsersSent < numUsers ) )
		while( ( (System.currentTimeMillis() - expStartTime) < WeatherAndMobilityBoth.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				sendAStateMessage((int)currUserGuidNum);
				currUserGuidNum++;
				currUserGuidNum=((int)currUserGuidNum)%WeatherAndMobilityBoth.numUsers;
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
				sendAStateMessage((int)currUserGuidNum);
				currUserGuidNum++;
				currUserGuidNum=((int)currUserGuidNum)%WeatherAndMobilityBoth.numUsers;
				//numSent++;
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("StateChange eventual sending rate "+sendingRate);
		
		waitForFinish();
		//Thread.sleep(WAIT_TIME);
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("StateChange result:Goodput "+sysThrput);
	}
	
	private void sendAStateMessage( int currUserGuidNum)
	{
		UserRecordInfo currUserInfo = WeatherAndMobilityBoth.userInfoHashMap.get
				(WeatherAndMobilityBoth.guidPrefix+currUserGuidNum);
		double state1Rand = stateChangeRand.nextDouble();
		
		// no state change
		if(state1Rand < 0.5)
		{
			numSent++;
			synchronized(waitLock)
			{
				numRecvd++;
			}
		}
		else // state change, need to decide which one with equal prob
		{
			state1Rand = stateChangeRand.nextDouble();
			int nextState = -1;
			
			if(state1Rand < 0.5)
			{
				if( currUserInfo.getUserActivity() == WeatherAndMobilityBoth.STATE_DRIVING )
				{
					nextState = WeatherAndMobilityBoth.STATE_WALKING;
				}
				else if( currUserInfo.getUserActivity() == WeatherAndMobilityBoth.STATE_WALKING )
				{
					nextState = WeatherAndMobilityBoth.STATE_STATIONARY;
				}
				else if( currUserInfo.getUserActivity() == WeatherAndMobilityBoth.STATE_STATIONARY )
				{
					nextState = WeatherAndMobilityBoth.STATE_DRIVING;
				}
			}
			else
			{
				if( currUserInfo.getUserActivity() == WeatherAndMobilityBoth.STATE_DRIVING )
				{
					nextState = WeatherAndMobilityBoth.STATE_STATIONARY;
				}
				else if( currUserInfo.getUserActivity() == WeatherAndMobilityBoth.STATE_WALKING )
				{
					nextState = WeatherAndMobilityBoth.STATE_DRIVING;
				}
				else if( currUserInfo.getUserActivity() == WeatherAndMobilityBoth.STATE_STATIONARY )
				{
					nextState = WeatherAndMobilityBoth.STATE_WALKING;
				}
			}
			
			JSONObject attrValJSON = new JSONObject();
			try
			{
				attrValJSON.put(WeatherAndMobilityBoth.userStateAttrName, nextState);
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			
			//System.out.println("Updating "+currUserGuidNum+" "+attrValJSON);
			numSent++;
			UpdateTask updTask = new UpdateTask
					( attrValJSON, currUserInfo, this );
			WeatherAndMobilityBoth.taskES.execute(updTask);
		}
	}

	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			System.out.println("StateChange reply recvd "+userGUID+" time taken "+timeTaken+
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