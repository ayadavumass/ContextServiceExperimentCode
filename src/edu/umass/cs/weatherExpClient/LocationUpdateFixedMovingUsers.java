package edu.umass.cs.weatherExpClient;

import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.acs.geodesy.GeodeticCalculator;
import edu.umass.cs.acs.geodesy.GlobalCoordinate;

/**
 * Updates locations of all users after every 
 * granularityOfGeolocationUpdate
 * @author adipc
 */
public class LocationUpdateFixedMovingUsers extends AbstractRequestSendingClass implements Runnable
{
	private final Random updateRand;
	public LocationUpdateFixedMovingUsers()
	{
		super(WeatherAndMobilityBoth.UPD_LOSS_TOLERANCE);
		updateRand = new Random(WeatherAndMobilityBoth.myID);
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
		double reqspms = WeatherAndMobilityBoth.updateRate/1000.0;
		
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double currUserGuidNum   = 0;
		
		//while( ( totalNumUsersSent < numUsers ) )
		while
			( ( (System.currentTimeMillis() - expStartTime) < WeatherAndMobilityBoth.EXPERIMENT_TIME ) )
		{
			for( int i=0; i<numberShouldBeSentPerSleep; i++ )
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
		
		if( currUserInfo.getUserActivity() == WeatherAndMobilityBoth.STATE_DRIVING )
		{
			GlobalCoordinate gcord 
				= new GlobalCoordinate(currUserInfo.getLatitude(), currUserInfo.getLongitude());
			
			double movementAngle = currUserInfo.getAngleOfMovement();
			
			double distanceInMeters 
				= (WeatherAndMobilityBoth.SPEED_DRIVING * 1.6 * 1000.0)*(WeatherAndMobilityBoth.granularityOfGeolocationUpdate/(1000.0*3600));
			
			GlobalCoordinate endCoord = GeodeticCalculator.calculateEndingGlobalCoordinates
					(gcord, movementAngle, distanceInMeters);
			
			double newLat = endCoord.getLatitude(), newLong = endCoord.getLongitude();
			
			// containing user in the bounding box
			if( endCoord.getLatitude() <= WeatherAndMobilityBoth.LATITUDE_MIN || 
					endCoord.getLatitude() >= WeatherAndMobilityBoth.LATITUDE_MAX )
			{
				double latRand   = updateRand.nextDouble();
				double latDiff   = WeatherAndMobilityBoth.LATITUDE_MAX-WeatherAndMobilityBoth.LATITUDE_MIN;
				
				newLat = WeatherAndMobilityBoth.LATITUDE_MIN + latDiff * latRand;
			}
			
			if( endCoord.getLongitude() <= WeatherAndMobilityBoth.LONGITUDE_MIN || 
					endCoord.getLongitude() >= WeatherAndMobilityBoth.LONGITUDE_MAX )
			{
				double longRand  = updateRand.nextDouble();
				
				double longDiff  = WeatherAndMobilityBoth.LONGITUDE_MAX-WeatherAndMobilityBoth.LONGITUDE_MIN;
				
				newLong = WeatherAndMobilityBoth.LONGITUDE_MIN + longDiff * longRand;
			}
			endCoord = new GlobalCoordinate(newLat, newLong);
			
			currUserInfo.setGeoLocation(endCoord.getLatitude(), endCoord.getLongitude());
			
			JSONObject attrValJSON = new JSONObject();
			try
			{
				attrValJSON.put(WeatherAndMobilityBoth.latitudeAttrName, endCoord.getLatitude());
				attrValJSON.put(WeatherAndMobilityBoth.longitudeAttrName, endCoord.getLongitude());
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			
			UpdateTask updTask = new UpdateTask( attrValJSON, currUserInfo, this );
			WeatherAndMobilityBoth.taskES.execute(updTask);
		}
		else if( currUserInfo.getUserActivity() == WeatherAndMobilityBoth.STATE_WALKING )
		{
			GlobalCoordinate gcord 
			= new GlobalCoordinate(currUserInfo.getLatitude(), currUserInfo.getLongitude());
		
			double movementAngle = currUserInfo.getAngleOfMovement();
		
			double distanceInMeters 
				= (WeatherAndMobilityBoth.SPEED_WALKING * 1.6 * 1000.0)*(WeatherAndMobilityBoth.granularityOfGeolocationUpdate/(1000.0*3600));
		
			GlobalCoordinate endCoord = GeodeticCalculator.calculateEndingGlobalCoordinates
				(gcord, movementAngle, distanceInMeters);
			
			double newLat = endCoord.getLatitude(), newLong = endCoord.getLongitude();
			
			// containing user in the bounding box
			if( endCoord.getLatitude() <= WeatherAndMobilityBoth.LATITUDE_MIN || 
					endCoord.getLatitude() >= WeatherAndMobilityBoth.LATITUDE_MAX )
			{
				double latRand   = updateRand.nextDouble();
				double latDiff   = WeatherAndMobilityBoth.LATITUDE_MAX-WeatherAndMobilityBoth.LATITUDE_MIN;
				
				newLat = WeatherAndMobilityBoth.LATITUDE_MIN + latDiff * latRand;
			}
						
			if( endCoord.getLongitude() <= WeatherAndMobilityBoth.LONGITUDE_MIN || 
					endCoord.getLongitude() >= WeatherAndMobilityBoth.LONGITUDE_MAX )
			{
				double longRand  = updateRand.nextDouble();	
				double longDiff  = WeatherAndMobilityBoth.LONGITUDE_MAX-WeatherAndMobilityBoth.LONGITUDE_MIN;
				
				newLong = WeatherAndMobilityBoth.LONGITUDE_MIN + longDiff * longRand;
			}
			
			
			endCoord = new GlobalCoordinate(newLat, newLong);
			
			
			currUserInfo.setGeoLocation(endCoord.getLatitude(), endCoord.getLongitude());
		
			JSONObject attrValJSON = new JSONObject();
			try
			{
				attrValJSON.put(WeatherAndMobilityBoth.latitudeAttrName, endCoord.getLatitude());
				attrValJSON.put(WeatherAndMobilityBoth.longitudeAttrName, endCoord.getLongitude());
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			UpdateTask updTask = new UpdateTask( attrValJSON, currUserInfo, this );
			WeatherAndMobilityBoth.taskES.execute(updTask);
		}
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