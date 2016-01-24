package edu.umass.cs.weatherExpClient;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.acs.geodesy.GeodeticCalculator;
import edu.umass.cs.acs.geodesy.GlobalCoordinate;
import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.genericExpClient.JointRequestsNew2;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.exceptions.GnsException;


public class MobilityModel
{
	private static final double LONGITUDE_MIN 					= -98.08;
	private static final double LONGITUDE_MAX 					= -96.01;
	
	private static final double LATITUDE_MAX 					= 33.635;
	private static final double LATITUDE_MIN 					= 31.854;
	
	private static int numUsers 								= -1;
	
	private static String gnsHost 								= "";
	private static int gnsPort 									= -1;
	
	private static String csHost 								= "";
	private static int csPort 									= -1;
	
	private static final String guidPrefix						= "UserGUID";
	
	// state of a user
	private static final String STATE_DRIVING					= "Driving";
	private static final String STATE_WALKING					= "Walking";
	private static final String STATE_STATIONARY				= "Stationary";
	
	
	private static final int SPEED_DRIVING						= 40;  // 40 mph
	private static final int SPEED_WALKING						= 3;   // 3 mph
	private static final int SPEED_STATIONARY					= 0;
	
	//GNS fields
	public static final String GEO_LOCATION_CURRENT 			= "geoLocationCurrent"; // Users current location (dynamic)
	
	//CS fields
	private static final String latitudeAttrName				= "geoLocationCurrentLat";
	private static final String longitudeAttrName				= "geoLocationCurrentLong";
	
	//common to both
	private static final String userStateAttrName				= "userState";
	
	
	private static int granularityOfGeolocationUpdate			= 10000; //about every 10 sec
	
	private static int granularityOfStateChange					= 1*60*1000; //about every  1 min, will be changed to 15 min
	
	// it set to true then GNS is used for updates
	// otherwise updates are directly sent to context service.
	private static final boolean useGNS							= false;
	
	
	private final UniversalTcpClient gnsClient;
	private GuidEntry accountGuid;
	
	private HashMap<String, UserRecordInfo> userInfoHashMap;
	
	private Random generalRand;
	
	private final ExecutorService taskES;
	
	private static boolean useContextService					= false;
	private static boolean justInitialize						= false;
	
	private final ContextServiceClient<String> csClient;
	
	public static void main(String[] args) throws IOException
	{
		numUsers = Integer.parseInt(args[0]);
		gnsHost  = args[1];
		gnsPort  = Integer.parseInt(args[2]);
		csHost   = args[3];
		csPort   = Integer.parseInt(args[4]);
		useContextService = Boolean.parseBoolean(args[5]);
		justInitialize = Boolean.parseBoolean(args[6]);
		
		new MobilityModel();
	}
	
	public MobilityModel() throws IOException
	{
		gnsClient = new UniversalTcpClient(gnsHost, gnsPort, true);
		csClient = new ContextServiceClient<String>(csHost, csPort); 
		
		userInfoHashMap = new HashMap<String, UserRecordInfo>();
		
		generalRand = new Random();
		
		taskES = Executors.newCachedThreadPool();
		
		try
		{
			initialize();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		catch(Error ex)
		{
			ex.printStackTrace();
		}
		
		if( !justInitialize )
		{
			new Thread(new LocationUpdateThread()).start();
			new Thread(new StateChangeThread()).start();
		}
	}
	
	/**
	 * Initialize all users
	 * @throws Exception 
	 */
	public void initialize() throws Exception
	{
		accountGuid = gnsClient.accountGuidCreate("gnsumass@gmail.com", "testPass");
		Thread.sleep(5000);
		System.out.println("account guid created "+accountGuid.getGuid());
	
		for( int i=0;i<numUsers;i++ )
		{
			GuidEntry userGuidEntry = gnsClient.guidCreate(accountGuid, guidPrefix+i);
			
			double latRand   = generalRand.nextDouble();
			double longRand  = generalRand.nextDouble();
			double latDiff   = LATITUDE_MAX-LATITUDE_MIN;
			double longDiff  = LONGITUDE_MAX-LONGITUDE_MIN;
			
			double userLat   = LATITUDE_MIN + latDiff * latRand;
			double userLong  = LONGITUDE_MIN + longDiff * longRand;
			
			String userState="";
			
			if( (i%3) == 0 )
			{
				userState = STATE_DRIVING;
			}
			else if( (i%3) == 1 )
			{
				userState = STATE_WALKING;
			}
			else if( (i%3) == 2 )
			{
				userState = STATE_STATIONARY;
			}
			
			JSONObject attrValJSON = null;
			
			if( this.useContextService )
			{
				attrValJSON = getUpdateJSONForCS(userState, userLat, userLong);
			}
			else
			{
				attrValJSON = getUpdateJSONForGNS(userState, userLat, userLong);
			}
			
			// angle is between 0 to 360
			// it is set once for each state of user activity
			double angleOfMovement = generalRand.nextDouble()*360;
			String userGUID = "";
			if( useGNS )
			{
				userGUID = userGuidEntry.getGuid();
			}
			else
			{
				userGUID = getSHA1(guidPrefix+i);
			}
			
			UserRecordInfo userRecordInfo 
				= new UserRecordInfo(guidPrefix+i, userGuidEntry, userGUID);
			userRecordInfo.setGeoLocation(userLat, userLong);
			userRecordInfo.setUserActivity(userState);
			userRecordInfo.setAngleOfMovement(angleOfMovement);
			
			userInfoHashMap.put(guidPrefix+i, userRecordInfo);
			
			System.out.println("Initializing "+userGuidEntry.getGuid()+" "+attrValJSON);
			
			UpdateTask updTask = new UpdateTask(attrValJSON, userRecordInfo);
			taskES.execute(updTask);
		}
	}
	
	
	private void rateControlledRequestSender() throws Exception
	{
		Integer clientID = Integer.parseInt(args[0]);
		writerName = "writer"+clientID;
		NUMATTRs = Integer.parseInt(args[1]);
		requestsps = Double.parseDouble(args[2]);
		
		// calculated by query/(query+update)
		queryUpdateRatio = Double.parseDouble(args[3]);
		
		NUMGUIDs = Integer.parseInt(args[4]);
		
		queryUpdateRand  = new Random(clientID);
		
		JointRequestsNew2<Integer> basicObj 
														= new JointRequestsNew2<Integer>(clientID);
		
		
		// should be less than 1000, for more more processes of this should be started
		//int waitInLoop = 1000/requestsps;
		long currTime = 0;
		expStartTime = System.currentTimeMillis();
		double numberShouldBeSentPerSleep = requestsps/10.0;
		
		
		while( ( (System.currentTimeMillis() - expStartTime) < EXPERIMENT_TIME ) )
		{
			for(int i=0;i<numberShouldBeSentPerSleep;i++)
			{
				sendAMessage(basicObj);
				currNumReqSent++;
			}
			currTime = System.currentTimeMillis();
			
			double timeElapsed = ((currTime- expStartTime)*1.0)/1000.0;
			double numberShouldBeSentByNow = timeElapsed*requestsps;
			double needsToBeSentBeforeSleep = numberShouldBeSentByNow - currNumReqSent;
			if(needsToBeSentBeforeSleep > 0)
			{
				needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
			}
			
			for(int i=0;i<needsToBeSentBeforeSleep;i++)
			{
				sendAMessage(basicObj);
				currNumReqSent++;
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (currNumReqSent * 1.0)/(timeInSec);
		System.out.println("Eventual sending rate "+sendingRate);
		
		basicObj.waitForFinish();
		//Thread.sleep(WAIT_TIME);
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (currNumReplyRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Result:Goodput "+sysThrput);
		
		System.exit(0);
	}
	
	
	private JSONObject getUpdateJSONForCS(String userState, double userLat, 
			double userLong) throws JSONException
	{
		JSONObject attrValJSON = new JSONObject();
		attrValJSON.put(latitudeAttrName, userLat);
		attrValJSON.put(longitudeAttrName, userLong);
		attrValJSON.put(userStateAttrName, userState);
		return attrValJSON;
	}
	
	private JSONObject getUpdateJSONForGNS(String userState, double userLat, 
			double userLong) throws JSONException
	{
		JSONObject attrValJSON = new JSONObject();
		attrValJSON.put(GEO_LOCATION_CURRENT, 
				GeoJSON.createGeoJSONCoordinate(new GlobalCoordinate(userLat, userLong)));
		attrValJSON.put(userStateAttrName, userState);
		return attrValJSON;
	}
	
	public static String getSHA1(String stringToHash)
	{
	   MessageDigest md=null;
	   try
	   {
		   md = MessageDigest.getInstance("SHA-256");
	   } catch (NoSuchAlgorithmException e)
	   {
		   e.printStackTrace();
	   }
       
	   md.update(stringToHash.getBytes());
 
       byte byteData[] = md.digest();
 
       //convert the byte to hex format method 1
       StringBuffer sb = new StringBuffer();
       for (int i = 0; i < byteData.length; i++) 
       {
       		sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
       }
       String returnGUID = sb.toString();
       return returnGUID.substring(0, 40);
	}
	
	/**
	 * Class implements the task used for 
	 * update info in GNS, which is blocking so this 
	 * class's object is passed in executor service
	 * @author adipc
	 */
	private class UpdateTask implements Runnable
	{
		private final JSONObject attrValuePairs;
		private final UserRecordInfo userRecordInfo;
		//private final GuidEntry guidEntry;
		//private final String userGUID;
		
		public UpdateTask(JSONObject attrValuePairs, UserRecordInfo userRecordInfo)
		{
			this.attrValuePairs = attrValuePairs;
			this.userRecordInfo = userRecordInfo;
			//this.guidEntry = guidEntry;
			//this.userGUID = guidEntry.getGuid();
		}
		
		@Override
		public void run()
		{
			try
			{
				if(useGNS)
				{
					GuidEntry guidEntry = userRecordInfo.getUserGuidEntry();
					long start = System.currentTimeMillis();
					System.out.println("GNS GUID "+guidEntry.getGuid() +" update starting "
							+System.currentTimeMillis() );
					gnsClient.update(guidEntry.getGuid(), attrValuePairs, guidEntry);
					long end = System.currentTimeMillis();
					System.out.println("GNS GUID "+guidEntry.getGuid() +" update ending "
							+System.currentTimeMillis() +"time taken" +(end-start));
				}
				else
				{
					String userGUID = userRecordInfo.getGUIDString();
					long start = System.currentTimeMillis();
					System.out.println("CS GUID "+userGUID +" update starting "
							+System.currentTimeMillis() );
					csClient.sendUpdate(userGUID, attrValuePairs, -1, true);
					long end = System.currentTimeMillis();
					System.out.println("CS GUID "+userGUID +" update ending "
							+System.currentTimeMillis() +" time taken" +(end-start));
				}
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (GnsException e)
			{
				e.printStackTrace();
			} catch(Exception ex)
			{
				ex.printStackTrace();
			}
			catch(Error ex)
			{
				ex.printStackTrace();
			}
		}
	}
	
	/**
	 * Updates locations of all users after every 
	 * granularityOfGeolocationUpdate
	 * @author adipc
	 */
	private class LocationUpdateThread implements Runnable
	{
		@Override
		public void run()
		{
			while( true )
			{
				try
				{
					locationUpdateFunction();
				}
				catch(Exception ex)
				{
					ex.printStackTrace();
				}
				catch(Error ex)
				{
					ex.printStackTrace();
				}
				
				try
				{
					Thread.sleep(granularityOfGeolocationUpdate);
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
		
		private void locationUpdateFunction()
		{
			for( int i=0;i<numUsers;i++ )
			{
				UserRecordInfo currUserInfo = userInfoHashMap.get(guidPrefix+i);
				
				if( currUserInfo.getUserActivity().equals(STATE_DRIVING) )
				{
					GlobalCoordinate gcord 
						= new GlobalCoordinate(currUserInfo.getLatitude(), currUserInfo.getLongitude());
					
					double movementAngle = currUserInfo.getAngleOfMovement();
					
					double distanceInMeters 
						= (SPEED_DRIVING * 1.6 * 1000.0)*(granularityOfGeolocationUpdate/(1000.0*3600));
					
					GlobalCoordinate endCoord = GeodeticCalculator.calculateEndingGlobalCoordinates
							(gcord, movementAngle, distanceInMeters);
					
					currUserInfo.setGeoLocation(endCoord.getLatitude(), endCoord.getLongitude());
					
					JSONObject attrValJSON = new JSONObject();
					try
					{
						attrValJSON.put(latitudeAttrName, endCoord.getLatitude());
						attrValJSON.put(longitudeAttrName, endCoord.getLongitude());
					} catch (JSONException e)
					{
						e.printStackTrace();
					}
					System.out.println("Updating "+i+" "+attrValJSON);
					
					UpdateTask updTask = new UpdateTask( attrValJSON, currUserInfo );
					taskES.execute(updTask);
				}
				else if( currUserInfo.getUserActivity().equals(STATE_WALKING) )
				{
					GlobalCoordinate gcord 
					= new GlobalCoordinate(currUserInfo.getLatitude(), currUserInfo.getLongitude());
				
					double movementAngle = currUserInfo.getAngleOfMovement();
				
					double distanceInMeters 
						= (SPEED_WALKING * 1.6 * 1000.0)*(granularityOfGeolocationUpdate/(1000.0*3600));
				
					GlobalCoordinate endCoord = GeodeticCalculator.calculateEndingGlobalCoordinates
						(gcord, movementAngle, distanceInMeters);
				
					currUserInfo.setGeoLocation(endCoord.getLatitude(), endCoord.getLongitude());
				
					JSONObject attrValJSON = new JSONObject();
					try
					{
						attrValJSON.put(latitudeAttrName, endCoord.getLatitude());
						attrValJSON.put(longitudeAttrName, endCoord.getLongitude());
					} catch (JSONException e)
					{
						e.printStackTrace();
					}
					System.out.println("Updating "+i+" "+attrValJSON);
					UpdateTask updTask = new UpdateTask( attrValJSON, currUserInfo );
					taskES.execute(updTask);
				}
				else if( currUserInfo.getUserActivity().equals(STATE_STATIONARY) )
				{
					
				}
			}
		}
	}
	
	private class StateChangeThread implements Runnable
	{
		private Random stateChangeRand;
		
		public StateChangeThread()
		{
			stateChangeRand = new Random();
		}
		
		@Override
		public void run() 
		{
			while(true)
			{
				try
				{
					stateChangeFunction();
				}
				catch(Exception ex)
				{
					ex.printStackTrace();
				}
				catch(Error ex)
				{
					ex.printStackTrace();
				}
				
				try 
				{
					Thread.sleep(granularityOfStateChange);
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
		
		private void stateChangeFunction()
		{
			for( int i=0;i<numUsers;i++ )
			{
				UserRecordInfo currUserInfo = userInfoHashMap.get(guidPrefix+i);
				double state1Rand = stateChangeRand.nextDouble();
				
				// no state change
				if(state1Rand < 0.5)
				{	
				}
				else // state change, need to decide which one with equal prob
				{
					state1Rand = stateChangeRand.nextDouble();
					String nextState = "";
					
					if(state1Rand < 0.5)
					{
						if( currUserInfo.getUserActivity().equals(STATE_DRIVING) )
						{
							nextState = STATE_WALKING;
						}
						else if( currUserInfo.getUserActivity().equals(STATE_WALKING) )
						{
							nextState = STATE_STATIONARY;
						}
						else if( currUserInfo.getUserActivity().equals(STATE_STATIONARY) )
						{
							nextState = STATE_DRIVING;
						}
					}
					else
					{
						if( currUserInfo.getUserActivity().equals(STATE_DRIVING) )
						{
							nextState = STATE_STATIONARY;
						}
						else if( currUserInfo.getUserActivity().equals(STATE_WALKING) )
						{
							nextState = STATE_DRIVING;
						}
						else if( currUserInfo.getUserActivity().equals(STATE_STATIONARY) )
						{
							nextState = STATE_WALKING;
						}
					}
					
					JSONObject attrValJSON = new JSONObject();
					try
					{
						attrValJSON.put(userStateAttrName, nextState);
					} catch (JSONException e)
					{
						e.printStackTrace();
					}
					
					System.out.println("Updating "+i+" "+attrValJSON);
				
					UpdateTask updTask = new UpdateTask( attrValJSON, currUserInfo );
					taskES.execute(updTask);
				}
			}
		}
	}
	
}