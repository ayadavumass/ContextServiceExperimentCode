package edu.umass.cs.weathercasestudy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.acs.geodesy.GeodeticCalculator;
import edu.umass.cs.acs.geodesy.GeodeticCurve;
import edu.umass.cs.acs.geodesy.GlobalCoordinate;
import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.utils.UtilFunctions;

public class IssueUpdates extends AbstractRequestSendingClass
{
	public static final double UPD_LOSS_TOLERANCE       = 0.0;
	
	public static final int WAIT_TIME					= 100000;
	
	// 42.87417896666666 | 43.260640499999994 | -79.30631786666666 | -78.66029963333332
	// 42.87417896666666 | 43.00299947777777 | -78.87563904444443 | -78.66029963333332 
	
	public static final double minBuffaloLat 			= 42.0;
	public static final double maxBuffaloLat 			= 44.0;
	
	public static final double minBuffaloLong			= -80.0;
	public static final double maxBuffaloLong 			= -78.0;
	
//	public static final double minBuffaloLat 			= 42.87417896666666;
//	public static final double maxBuffaloLat 			= 43.00299947777777;
//	
//	public static final double minBuffaloLong			= -78.87563904444443;
//	public static final double maxBuffaloLong 			= -78.66029963333332;
	
	
	public static final double timeContractionFactor 	= 17859.416666667;
	
	public static final double MIN_UNIX_TIME			= 1385770103;
	public static final double MAX_UNIX_TIME			= 1391127928;
	
	public static final String guidPrefix				= "GUID";
	
	public static final String latitudeAttr				= "latitude";
	public static final String longitudeAttr			= "longitude";
	
	
	public static ContextServiceClient<String> csClient;
	public static boolean useGNS						= false;
	
	public static final String nomadLogDataPath 		
									= "/users/ayadavum/nomadLog/loc_seq";
	
//	public static final String nomadLogDataPath 		
//										= "/home/adipc/Documents/nomadlogData/loc_seq";
	
	private HashMap<Integer, List<TrajectoryEntry>> userMobilityEntryHashMap;
	
	
	private HashMap<Integer, Integer> nextEntryToSendMap;
	
	//private HashMap<Integer, Integer> realIDToMobilityIdMap;
	// trajectories in this are laterally transformed version of 
	// mobility log trajectories.
	private HashMap<Integer, List<TrajectoryEntry>> realIDTrajectoryMap;
	
	private double simulatedTime;
	
	private long requestId;
	
	private long numUpdatesRecvd;
	private double sumUpdateLatency;
	
	private static String csHost;
	private static int csPort;
	
	
	private double minLatData;
	private double maxLatData;
	private double minLongData;
	private double maxLongData;
	
	private Random transformRand;
	
	private boolean useLateralTransfrom					= false;
	
	public static  int NUMUSERS							= 100;
	
	
	public IssueUpdates() throws NoSuchAlgorithmException, IOException
	{
		super( UPD_LOSS_TOLERANCE );
		userMobilityEntryHashMap = new HashMap<Integer, List<TrajectoryEntry>>();
		//realIDToMobilityIdMap    = new HashMap<Integer, Integer>();
		realIDTrajectoryMap		 = new HashMap<Integer, List<TrajectoryEntry>>();
		nextEntryToSendMap       = new HashMap<Integer, Integer>();
		
		if( csHost != null )
			csClient  = new ContextServiceClient<String>(csHost, csPort, 
						ContextServiceClient.HYPERSPACE_BASED_CS_TRANSFORM);
		
		transformRand = new Random();
	}
	
	private void readNomadLag() throws IOException
	{
		
		minLatData = maxBuffaloLat;
		maxLatData = minBuffaloLat;
		minLongData = maxBuffaloLong;
		maxLongData = minBuffaloLong;
		
		File file = new File("buffaloTrace.txt");
		
		// if file doesn't exists, then create it
		if ( !file.exists() ) 
		{
			file.createNewFile();
		}
			
		FileWriter fw 	  = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		
		BufferedReader br = null;
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader(new FileReader(nomadLogDataPath));
			
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				String[] parsed = sCurrentLine.split("\\|");
				
				int userId = Integer.parseInt(parsed[1]);
				long unixtimestamp = (long)Double.parseDouble(parsed[2]);
				double latitude = Double.parseDouble(parsed[3]);
				double longitude = Double.parseDouble(parsed[4]);
				
				if( (latitude >= minBuffaloLat) && (latitude <= maxBuffaloLat) 
						&& (longitude >= minBuffaloLong) && (longitude <= maxBuffaloLong) )
				{
					Date date = new Date(unixtimestamp*1000L); // *1000 is to convert seconds to millisecond
					String str = userId+","+unixtimestamp+","+latitude+","+longitude+","
																		+date.toGMTString()+"\n";
					bw.write(str);
				}
				else
				{
					continue;
				}
				
				TrajectoryEntry logEntryObj = new TrajectoryEntry(unixtimestamp, 
						latitude, longitude);
				
				List<TrajectoryEntry> userEventList = userMobilityEntryHashMap.get(userId);
				if( userEventList == null )
				{
					userEventList = new LinkedList<TrajectoryEntry>();
					userEventList.add(logEntryObj);
					userMobilityEntryHashMap.put(userId, userEventList);
				}
				else
				{
					userEventList.add(logEntryObj);
				}
				
				if(latitude < minLatData)
				{
					minLatData = latitude;
				}
				
				if(latitude > maxLatData)
				{
					maxLatData = latitude;
				}
				
				if(longitude < minLongData)
				{
					minLongData = longitude;
				}
				
				if(longitude > maxLongData )
				{
					maxLongData = longitude;
				}
			}
		} catch (IOException e) 
		{
			e.printStackTrace();
		} finally
		{
			try
			{
				if (br != null)
					br.close();
			} catch (IOException ex)
			{
				ex.printStackTrace();
			}
		}
		bw.close();
		System.out.println( "unique users "
							+userMobilityEntryHashMap.size() );
		
		Iterator<Integer> userIdIter = userMobilityEntryHashMap.keySet().iterator();
		
		while( userIdIter.hasNext() )
		{
			int userId = userIdIter.next();
			
			List<TrajectoryEntry> logEntryList 
							= userMobilityEntryHashMap.get(userId);
			
			logEntryList.sort
			((o1, o2) -> o1.getUnixTimeStamp().compareTo(o2.getUnixTimeStamp()));
		}
	}
	
//	private void assignMobilityUserId()
//	{
//		for( int i=0; i<NUMUSERS; i++ )
//		{
//			int randMobIndex = rand.nextInt( userMobilityEntryHashMap.size() );
//			Iterator<Integer> userIdIter = userMobilityEntryHashMap.keySet().iterator();
//			int curr = 0;
//			Integer mobilityId = -1;
//			while( userIdIter.hasNext() )
//			{
//				mobilityId = userIdIter.next();
//				if( randMobIndex == curr )
//				{
//					break;
//				}
//				curr++;
//			}
//			
//			realIDToMobilityIdMap.put(i, mobilityId);
//			lastEntrySentMap.put(i, -1);
//		}
//	}
	
	private void createTransformedTrajectories()
	{	
		Iterator<Integer> logIdIter 
							= userMobilityEntryHashMap.keySet().iterator();
		
		for( int i=0; i<NUMUSERS; i++ )
		{
			// assign original trajectories first
			// and after that we assign laterally transformed trajectories.
			if( i < userMobilityEntryHashMap.size() )
			{
				if( logIdIter.hasNext() )
				{
					int logId = logIdIter.next();
					List<TrajectoryEntry> trajEntry 
										= userMobilityEntryHashMap.get(logId);
					
					realIDTrajectoryMap.put(i, trajEntry);
				}
				else
				{
					assert(false);
				}
			}
			else
			{
				int randLogIndex = transformRand.nextInt( userMobilityEntryHashMap.size() );
				Iterator<Integer> userIdIter = userMobilityEntryHashMap.keySet().iterator();
				int curr = 0;
				Integer logId = -1;
				while( userIdIter.hasNext() )
				{
					logId = userIdIter.next();
					if( randLogIndex == curr )
					{
						break;
					}
					curr++;
				}
				
				List<TrajectoryEntry> transformedTrajList = null;
				
				if(this.useLateralTransfrom)
				{
					transformedTrajList 
							= performUserTrajectoryTransformation( logId);
				}
				else
				{
					transformedTrajList = userMobilityEntryHashMap.get(logId);
				}
				
				realIDTrajectoryMap.put(i, transformedTrajList);
				
			}
			nextEntryToSendMap.put(i, 0);
		}
	}
	
	
//	public static final double minBuffaloLat 			= 42.0;
//	public static final double maxBuffaloLat 			= 44.0;
//	
//	public static final double minBuffaloLong			= -80.0;
//	public static final double maxBuffaloLong 			= -78.0;
	
	private List<TrajectoryEntry> performUserTrajectoryTransformation
										( int nomadLogUserId )
	{
		List<TrajectoryEntry> logTrajectory 
						= userMobilityEntryHashMap.get(nomadLogUserId);
		
		List<TrajectoryEntry> realUserTraj = new LinkedList<TrajectoryEntry>();
		
		double startAngle = -1;
		double endAngle = -1;
		double distanceInMeters = -1;
		
		for( int i=0; i<logTrajectory.size(); i++ )
		{
			TrajectoryEntry logEntry = logTrajectory.get(i);
			long unixTime = logEntry.getUnixTimeStamp();
			double latitude = logEntry.getLatitude();
			double longitude = logEntry.getLongitude();
			
			GlobalCoordinate logCoord 
								= new GlobalCoordinate(latitude, longitude);
			GlobalCoordinate transformedCoord = null;
			if( i == 0 )
			{
				double transformedLat = minBuffaloLat 
						+ transformRand.nextDouble()*(maxBuffaloLat-minBuffaloLat);
				double transformedLong = minBuffaloLong 
						+ transformRand.nextDouble()*(maxBuffaloLong-minBuffaloLong);
				
				
				transformedCoord = new GlobalCoordinate(transformedLat, transformedLong);
				
				GeodeticCurve gCurve 
						= GeodeticCalculator.calculateGeodeticCurve(logCoord, transformedCoord);
				
				startAngle = gCurve.getAzimuth();
				endAngle = gCurve.getReverseAzimuth();
				distanceInMeters = gCurve.getEllipsoidalDistance();
				
				TrajectoryEntry trajEntry 
						= new TrajectoryEntry(unixTime, transformedLat, transformedLong);
				
				realUserTraj.add(trajEntry);
			}
			else
			{
				transformedCoord = GeodeticCalculator.calculateEndingGlobalCoordinates
											(logCoord, startAngle, distanceInMeters);
				
				TrajectoryEntry trajEntry 
					= new TrajectoryEntry(unixTime, transformedCoord.getLatitude(), 
							transformedCoord.getLongitude());
				
				realUserTraj.add(trajEntry);				
			}
			
			if(transformedCoord.getLatitude() < minLatData)
			{
				minLatData = transformedCoord.getLatitude();
			}
			
			if(transformedCoord.getLatitude() > maxLatData)
			{
				maxLatData = transformedCoord.getLatitude();
			}
			
			if(transformedCoord.getLongitude() < minLongData)
			{
				minLongData = transformedCoord.getLongitude();
			}
			
			if(transformedCoord.getLongitude() > maxLongData )
			{
				maxLongData = transformedCoord.getLongitude();
			}
		}
		
		assert( logTrajectory.size() == realUserTraj.size() );
		return realUserTraj;
	}
	
	
	private void runUpdates() throws InterruptedException
	{
		simulatedTime = MIN_UNIX_TIME;
		while( simulatedTime <= MAX_UNIX_TIME )
		{
			Thread.sleep(1000);
			simulatedTime = simulatedTime +timeContractionFactor;
			sendUpdatesWhoseTimeHasCome(simulatedTime);
		}
	}
	
	
	private void sendUpdatesWhoseTimeHasCome(double simulatedTime)
	{
		Iterator<Integer> userIdIter = realIDTrajectoryMap.keySet().iterator();
		while( userIdIter.hasNext() )
		{
			int realId = userIdIter.next();
			List<TrajectoryEntry> trajList = realIDTrajectoryMap.get(realId);
			
			System.out.println("trajList "+trajList.size());
			
			int nextIndex = nextEntryToSendMap.get(realId);
			
			while( nextIndex < trajList.size() )
			{
				TrajectoryEntry trajEntry = trajList.get(nextIndex);
				nextIndex++;
				long currUnixTime = trajEntry.getUnixTimeStamp();
				if( currUnixTime <= simulatedTime )
				{
					sendUpdate(realId, trajEntry );
				}
				else
				{
					break;
				}
			}
			nextEntryToSendMap.put(realId, nextIndex);
		}
	}
	
	private void sendUpdate(int realId, TrajectoryEntry trajEntry )
	{
		String userGUID = "";
		if( useGNS )
		{
//			userGUID = userGuidEntry.getGuid();
		}
		else
		{
			userGUID = UtilFunctions.getGUIDHash(guidPrefix+realId);
		}
		
		JSONObject attrValJSON = new JSONObject();
		try
		{
			attrValJSON.put(latitudeAttr, trajEntry.getLatitude());
			attrValJSON.put(longitudeAttr, trajEntry.getLongitude());
		} 
		catch (JSONException e)
		{
			e.printStackTrace();
		}
		
		ExperimentUpdateReply updateRep = new ExperimentUpdateReply
												(requestId++, userGUID);
		
		System.out.println("requestId "+requestId+" realId "
							+realId+" attrValJSON "+attrValJSON);
		
		csClient.sendUpdateWithCallBack( userGUID, null, 
										attrValJSON, -1, updateRep, this.getCallBack() );
	}
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken)
	{
		synchronized(waitLock)
		{
			numRecvd++;
			numUpdatesRecvd++;
			System.out.println("Updates recvd "+userGUID+" time "+timeTaken
					+" numRecvd "+numRecvd);
			this.sumUpdateLatency = this.sumUpdateLatency + timeTaken;
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
	
	public static void main(String[] args)
				throws NoSuchAlgorithmException, 
				IOException, InterruptedException
	{
		csHost = args[0];
		csPort = Integer.parseInt(args[1]);
		NUMUSERS = Integer.parseInt(args[2]);
		
		//NUMUSERS = 1000;
		
		IssueUpdates issUpd = new IssueUpdates();
		issUpd.readNomadLag();
		//issUpd.assignMobilityUserId();
		issUpd.createTransformedTrajectories();
		System.out.println("minLatData "+issUpd.minLatData+" maxLatData "+issUpd.maxLatData
				+" minLongData "+issUpd.minLongData+" maxLongData "+issUpd.maxLongData);
		issUpd.runUpdates();
	}
}