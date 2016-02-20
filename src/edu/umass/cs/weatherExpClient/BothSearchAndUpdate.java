package edu.umass.cs.weatherExpClient;

import java.util.Random;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.acs.geodesy.GeodeticCalculator;
import edu.umass.cs.acs.geodesy.GlobalCoordinate;

public class BothSearchAndUpdate extends AbstractRequestSendingClass implements Runnable
{
	private final Random generalRand;
	
	private final Random latLowerValRand;
	private final Random latPredLenRand;
	
	private final Random longLowerValRand;
	private final Random longPredLenRand;
	
	private final Random updateRand;
	
	private double currUserGuidNum   = 0;
	public BothSearchAndUpdate()
	{
		super( WeatherAndMobilityBoth.UPD_LOSS_TOLERANCE );
		generalRand = new Random(WeatherAndMobilityBoth.myID);
		updateRand = new Random(WeatherAndMobilityBoth.myID);
		
		latLowerValRand = new Random(WeatherAndMobilityBoth.myID);
		latPredLenRand = new Random(WeatherAndMobilityBoth.myID);
		
		longLowerValRand = new Random(WeatherAndMobilityBoth.myID);
		longPredLenRand = new Random(WeatherAndMobilityBoth.myID);
	}
	
	@Override
	public void run()
	{
		try
		{
			this.startExpTime();
			rateControlledRequestSender();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void rateControlledRequestSender() throws Exception
	{
		double reqspms = WeatherAndMobilityBoth.updateRate/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		//while( ( totalNumUsersSent < numUsers ) )
		while( ( (System.currentTimeMillis() - expStartTime) < WeatherAndMobilityBoth.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				sendRequest();
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
				sendRequest();
				numSent++;
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Both eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Both result:Goodput "+sysThrput);
	}
	
	private void sendRequest()
	{
		// send update
		if(generalRand.nextDouble() < 0.5)
		{
			sendUpdate();
		}
		else
		{
			sendQueryMessage();
		}
	}
	
	private void sendUpdate()
	{
		sendALocMessage((int)currUserGuidNum);
		currUserGuidNum++;
		currUserGuidNum=((int)currUserGuidNum)%WeatherAndMobilityBoth.numUsers;
	}
	
	private void sendQueryMessage()
	{
		double latitudeMin = WeatherAndMobilityBoth.LATITUDE_MIN 
				+latLowerValRand.nextDouble()*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
		
		double predLength 
		= (latPredLenRand.nextDouble()*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN));
		
		double latitudeMax = latitudeMin + predLength;
//		double latitudeMax = latitudeMin 
//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
		// making it curcular
		if(latitudeMax > WeatherAndMobilityBoth.LATITUDE_MAX)
		{
			double diff = latitudeMax - WeatherAndMobilityBoth.LATITUDE_MAX;
			latitudeMax = WeatherAndMobilityBoth.LATITUDE_MIN + diff;
		}
		
		
		double longitudeMin = WeatherAndMobilityBoth.LONGITUDE_MIN 
				+longLowerValRand.nextDouble()*(WeatherAndMobilityBoth.LONGITUDE_MAX - WeatherAndMobilityBoth.LONGITUDE_MIN);
		
		predLength = (longPredLenRand.nextDouble()*(WeatherAndMobilityBoth.LONGITUDE_MAX - WeatherAndMobilityBoth.LONGITUDE_MIN));
		double longitudeMax = longitudeMin + predLength;
//		double longitudeMax = WeatherAndMobilityBoth.LONGITUDE_MIN 
//				+queryRand.nextDouble()*(WeatherAndMobilityBoth.LONGITUDE_MAX - WeatherAndMobilityBoth.LONGITUDE_MIN);
//		double longitudeMax = longitudeMin 
//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LONGITUDE_MAX - WeatherAndMobilityBoth.LONGITUDE_MIN);
		
		if( longitudeMax > WeatherAndMobilityBoth.LONGITUDE_MAX )
		{
			double diff = longitudeMax - WeatherAndMobilityBoth.LONGITUDE_MAX;
			longitudeMax = WeatherAndMobilityBoth.LONGITUDE_MIN + diff;
		}

		String searchQuery
			= "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE "
				+ "geoLocationCurrentLat >= "+latitudeMin +" AND geoLocationCurrentLat <= "+latitudeMax 
				+ " AND "
				+ "geoLocationCurrentLong >= "+longitudeMin+" AND geoLocationCurrentLong <= "+longitudeMax;
		
//			String searchQuery
//				= "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap(geoLocationCurrentLat, geoLocationCurrentLong, "+queryGeoJSON.toString()+")";
		SearchTask searchTask = new SearchTask( searchQuery, new JSONArray(), this );
		WeatherAndMobilityBoth.taskES.execute(searchTask);
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
		synchronized(waitLock)
		{
			numRecvd++;
			System.out.println("Search reply recvd size "+resultSize+" time taken "+timeTaken+
					" numSent "+numSent+" numRecvd "+numRecvd);
			//if(currNumReplyRecvd == currNumReqSent)
			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				waitLock.notify();
			}
		}
	}
}