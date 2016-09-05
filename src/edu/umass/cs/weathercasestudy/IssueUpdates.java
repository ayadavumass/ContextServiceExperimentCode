package edu.umass.cs.weathercasestudy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.TimeZone;

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
	
	public static final int WAIT_TIME					= 100000000;
	
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
	
	private double sumUpdatesPerUserAtOnce;
	private long counter;
	
	private Random transformRand;
	
	private boolean useLateralTransfrom					= true;
	private boolean useRealTraj							= false;
	
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
	
	private void createTransformedTrajectories()
	{	
		Iterator<Integer> logIdIter 
							= userMobilityEntryHashMap.keySet().iterator();
		
		for( int i=0; i<NUMUSERS; i++ )
		{
			// assign original trajectories first
//			// and after that we assign laterally transformed trajectories.
			if( useRealTraj && (i < userMobilityEntryHashMap.size()) )
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
					boolean succTransform;
					do
					{
						transformedTrajList = new LinkedList<TrajectoryEntry>();
						succTransform = performUserTrajectoryTransformation
								( logId, transformedTrajList);
					} while(!succTransform);
//					System.out.println("Transform list "+this.getTrajListString
//							(transformedTrajList));
				}
				else
				{
					transformedTrajList = userMobilityEntryHashMap.get(logId);
				}
				
				realIDTrajectoryMap.put(i, transformedTrajList);
				
			}
			nextEntryToSendMap.put(i, 0);
		}
		
		assert(realIDTrajectoryMap.size() == NUMUSERS);
	}
	
	private String getTrajListString(List<TrajectoryEntry> trajList)
	{
		String str = "";
		for(int i=0; i<trajList.size(); i++)
		{
			str = str+ trajList.get(i).getLatitude()+ " , "
								+trajList.get(i).getLongitude()+" ; ";
		}
		return str;
	}
	
	
//	public static final double minBuffaloLat 			= 42.0;
//	public static final double maxBuffaloLat 			= 44.0;
//	
//	public static final double minBuffaloLong			= -80.0;
//	public static final double maxBuffaloLong 			= -78.0;
	
	private boolean performUserTrajectoryTransformation
							( int nomadLogUserId, List<TrajectoryEntry> trajList )
	{
		boolean succTransform = true;
		List<TrajectoryEntry> logTrajectory 
						= userMobilityEntryHashMap.get(nomadLogUserId);
		
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
				
				if(!isCoordWithinBounds(transformedCoord.getLatitude(), 
						transformedCoord.getLongitude()))
				{
					succTransform = false;
				}
				
				TrajectoryEntry trajEntry 
						= new TrajectoryEntry(unixTime, transformedLat, transformedLong);
				
				trajList.add(trajEntry);
			}
			else
			{
				transformedCoord = GeodeticCalculator.calculateEndingGlobalCoordinates
											(logCoord, startAngle, distanceInMeters);
				
				if(!isCoordWithinBounds(transformedCoord.getLatitude(), 
						transformedCoord.getLongitude()))
				{
					succTransform = false;
				}
				
				TrajectoryEntry trajEntry 
					= new TrajectoryEntry(unixTime, transformedCoord.getLatitude(), 
							transformedCoord.getLongitude());
				
				trajList.add(trajEntry);				
			}
			
		}
		
		assert( logTrajectory.size() == trajList.size() );
		return succTransform;
	}
	
	private boolean isCoordWithinBounds(double latitude, double longitude)
	{
		if( (latitude >= minBuffaloLat) && (latitude <= maxBuffaloLat) 
				&& (longitude >= minBuffaloLong) && (longitude <= maxBuffaloLong) )
		{
			return true;
		}
		else
			return false;
		
	}
	
	private void runUpdates() throws InterruptedException
	{
		long start = System.currentTimeMillis();
		simulatedTime = MIN_UNIX_TIME;
		while( simulatedTime <= MAX_UNIX_TIME )
		{
			Date date = new Date((long)simulatedTime*1000L); // *1000 is to convert seconds to milliseconds
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); // the format of your date
			sdf.setTimeZone(TimeZone.getTimeZone("GMT")); // give a timezone reference for formating (see comment at the bottom
			String dateFormat = sdf.format(date);
			System.out.println("Update current simulated time "+simulatedTime+" time in GMT "
					+dateFormat+" numSent "+numSent+" numRecvd "+numRecvd+ "updatesAtSameTime "
					+(sumUpdatesPerUserAtOnce/counter));
			sendUpdatesWhoseTimeHasCome(simulatedTime);
			//Thread.sleep(1000);
			simulatedTime = simulatedTime +timeContractionFactor;
			
		}
		long end = System.currentTimeMillis();
		double sendingRate = (numSent*1000.0)/(end-start);
		System.out.println("Update eventual sending rate "+sendingRate+" reqs/s");
		this.waitForFinish();
		long endTime = System.currentTimeMillis();
		double systemThpt = (numRecvd*1000.0)/(endTime-start);
		System.out.println("Update system throughput "+systemThpt+" reqs/s");
		System.out.println("Update avg update latency "+(sumUpdateLatency/numRecvd)+" ms");
	}
	
	private void sendUpdatesWhoseTimeHasCome(double simulatedTime)
	{
		HashMap<Integer, Queue<UpdateInfo>> currUpdatesMap 
							= new HashMap<Integer, Queue<UpdateInfo>>();
		int totalUpdates = 0;
		
		Iterator<Integer> userIdIter = realIDTrajectoryMap.keySet().iterator();
		while( userIdIter.hasNext() )
		{
			int realId = userIdIter.next();
			List<TrajectoryEntry> trajList = realIDTrajectoryMap.get(realId);
			
			int nextIndex = nextEntryToSendMap.get(realId);
			
			int updatesAtSameTime = 0;
			
			while( nextIndex < trajList.size() )
			{
				TrajectoryEntry trajEntry = trajList.get(nextIndex);
				nextIndex++;
				long currUnixTime = trajEntry.getUnixTimeStamp();
				if( currUnixTime <= simulatedTime )
				{
					//sendUpdate(realId, trajEntry );
					totalUpdates++;
					if( currUpdatesMap.containsKey(realId) )
					{
						Queue<UpdateInfo> guidUpdateList = currUpdatesMap.get(realId);
						UpdateInfo updInfo = new UpdateInfo(realId, trajEntry);
						guidUpdateList.add(updInfo);
					}
					else
					{
						Queue<UpdateInfo> guidUpdateList = new LinkedList<UpdateInfo>();
						UpdateInfo updInfo = new UpdateInfo(realId, trajEntry);
						guidUpdateList.add(updInfo);
						currUpdatesMap.put(realId, guidUpdateList);
					}
					updatesAtSameTime++;
				}
				else
				{
					break;
				}
			}
			if(updatesAtSameTime > 0)
			{
				sumUpdatesPerUserAtOnce = sumUpdatesPerUserAtOnce + updatesAtSameTime;
				counter++;
			}
			nextEntryToSendMap.put(realId, nextIndex);
		}
		
		sendStaggeredUpdates(currUpdatesMap, totalUpdates);
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
		
//		System.out.println("requestId "+requestId+" realId "
//							+realId+" attrValJSON "+attrValJSON);
		numSent++;
		csClient.sendUpdateWithCallBack( userGUID, null, 
										attrValJSON, -1, updateRep, this.getCallBack() );
	}
	
	
	private void sendStaggeredUpdates( HashMap<Integer, Queue<UpdateInfo>> currUpdatesMap, 
			int totalUpdates )
	{
		double timeToRunUpdates = 1000.0; // 1000 ms
		double sleepTime = 100.0;
		
		double numIterations = timeToRunUpdates/sleepTime;
		
		if( totalUpdates <=  numIterations)
		{
			// execute all at once and then wait for timeToRunUpdates
			while( !sendRoundRobinUpdates( totalUpdates,  currUpdatesMap ) )
			{
				try 
				{
					Thread.sleep((long) timeToRunUpdates);
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
			
		}
		else
		{
			// execute staggered with sleeping of sleepTime
			double numUpdPerSleep = (totalUpdates*1.0)/numIterations;
			
			for(int i=0; i<numIterations; i++)
			{
				sendRoundRobinUpdates( numUpdPerSleep,  currUpdatesMap );
				try 
				{
					Thread.sleep((long) sleepTime);
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
			// send remanining quickly
			while( !sendRoundRobinUpdates( numUpdPerSleep,  currUpdatesMap ) )
			{
			}
		}
	}
	
	
	private boolean sendRoundRobinUpdates( double numUpdPerSleep, 
			HashMap<Integer, Queue<UpdateInfo>> currUpdatesMap )
	{
		boolean empty = true;
		int totalUpdates = (int)Math.ceil(numUpdPerSleep);
		int numberSentSoFar = 0;
		do
		{
			empty = true;
			Iterator<Integer> userIdIter = currUpdatesMap.keySet().iterator();
			
			while( userIdIter.hasNext() )
			{
				int realId = userIdIter.next();
				Queue<UpdateInfo> userUpdateList = currUpdatesMap.get(realId);
				
				if( userUpdateList.size() > 0 )
				{
					UpdateInfo updInfo = userUpdateList.poll();
					
					sendUpdate(updInfo.getRealId(), updInfo.getTrajEntry() );
					numberSentSoFar++;
					
					if( numberSentSoFar >= totalUpdates )
					{
						break;
					}			
				}
				
				if( userUpdateList.size() > 0 )
					empty = false;
			}
			
			if( numberSentSoFar >= totalUpdates )
			{
				break;
			}
		}
		while(!empty);
		return empty;
	}
	
	
	public void printLogStats()
	{
		// 1st partition is : 42 | 42.666666666666664 | -80 | -79.33333333333333
		// 2nd partition is : 42 | 42.666666666666664 | -79.33333333333333 | -78.66666666666666
		// 3rd partition is : 42 | 42.666666666666664 | -78.66666666666666 | -78
		// 4th partition is : 42.666666666666664 |  43.33333333333333 | -80 | -79.33333333333333
		// 5th partition is : 42.666666666666664 |  43.33333333333333 | -79.33333333333333 | -78.66666666666666
		// 6th partition is : 42.666666666666664 |  43.33333333333333 | -78.66666666666666 | -78
		// 7th partition is : 43.33333333333333 |  43.99999999999999 | -80 | -79.33333333333333
		// 8th partition is : 43.33333333333333 |  43.99999999999999 | -79.33333333333333 | -78.66666666666666
		// 9th partition is : 43.33333333333333 |  43.99999999999999 | -78.66666666666666 | -78
		
		HashMap<Integer, Long> logStatMap = new HashMap<Integer, Long>();
		Iterator<Integer> logIdIter = userMobilityEntryHashMap.keySet().iterator();
		
		while( logIdIter.hasNext() )
		{
			int logId = logIdIter.next();
			List<TrajectoryEntry> logTrajEntry = userMobilityEntryHashMap.get(logId);
			
			for(int i=0; i<logTrajEntry.size(); i++)
			{
				TrajectoryEntry trajEntry = logTrajEntry.get(i);
				int partitionNum = getPartitionSatisfiedByEntry( trajEntry );
				
				if(partitionNum == -1)
				{
					assert(false);
				}
				else
				{
					if( logStatMap.containsKey(partitionNum) )
					{
						long counter = logStatMap.get(partitionNum);
						counter++;
						logStatMap.put(partitionNum, counter);
					}
					else
					{
						//partitionNum
						logStatMap.put(partitionNum, (long)1);
					}
				}
			}
		}
		
		Iterator<Integer> logStatMapIter = logStatMap.keySet().iterator();
		
		while( logStatMapIter.hasNext() )
		{
			int partitionNum = logStatMapIter.next();
			long numEntries = logStatMap.get(partitionNum);
			System.out.println("Log stat partitionNum "+partitionNum
					+" numEntries "+numEntries);
		}
	}
	
	
	public void printRealUserStats()
	{
		HashMap<Integer, Long> realUserStatMap = new HashMap<Integer, Long>();
		Iterator<Integer> realIdIter = realIDTrajectoryMap.keySet().iterator();
		System.out.println(" realIDTrajectoryMap size "+ realIDTrajectoryMap.size());
		
		while( realIdIter.hasNext() )
		{
			int realId = realIdIter.next();
			//System.out.println("Executing realId "+realId);
			List<TrajectoryEntry> realTrajList = realIDTrajectoryMap.get(realId);
			
			for(int i=0; i<realTrajList.size(); i++)
			{
				TrajectoryEntry trajEntry = realTrajList.get(i);
				int partitionNum = getPartitionSatisfiedByEntry( trajEntry );
				
				if(partitionNum == -1)
				{
					System.out.println(" trajEntry "+trajEntry.getLatitude()+" "+trajEntry.getLongitude()
								+" realId "+realId);
					assert(false);
				}
				else
				{
					if( realUserStatMap.containsKey(partitionNum) )
					{
						long counter = realUserStatMap.get(partitionNum);
						counter++;
						realUserStatMap.put(partitionNum, counter);
					}
					else
					{
						//partitionNum
						realUserStatMap.put(partitionNum, (long)1);
					}
				}
			}
		}
		
		System.out.println("realUserStatMap size "+realUserStatMap.size());
		Iterator<Integer> realIdStatMapIter = realUserStatMap.keySet().iterator();
		
		while( realIdStatMapIter.hasNext() )
		{
			int partitionNum = realIdStatMapIter.next();
			long numEntries = realUserStatMap.get(partitionNum);
			System.out.println("RealUser stat partitionNum "+partitionNum
					+" numEntries "+numEntries);
		}
	}
	
	private int getPartitionSatisfiedByEntry( TrajectoryEntry trajEntry )
	{
		double[] minLatArray = {42.0, 42.0, 42.0, 42.66, 42.66, 42.66, 43.33, 43.33, 43.33};
		double[] maxLatArray = {42.66, 42.66, 42.66, 43.33, 43.33, 43.33, 43.99, 43.99, 43.99};
		double[] minLongArray = {-80.0, -79.33, -78.66, -80.0, -79.33, -78.66, -80.0, -79.33, -78.66};
		double[] maxLongArray = {-79.33, -78.66, -78.0, -79.33, -78.66, -78.0, -79.33, -78.66, -78.0};
		
		for(int i=0; i<9; i++)
		{
			double minLat = minLatArray[i];
			double maxLat = maxLatArray[i];
			double minLong = minLongArray[i];
			double maxLong = maxLongArray[i];
			
			boolean ret = ifPartitionSatisfiesEntry( trajEntry, 
					minLat, maxLat, minLong, maxLong );
			if(ret)
			{
				return i+1;
			}
		}
		return -1;
	}
	
	private boolean ifPartitionSatisfiesEntry( TrajectoryEntry trajEntry, 
			double minLat, double maxLat, double minLong, double maxLong )
	{
		double currLat  = trajEntry.getLatitude();
		double currLong = trajEntry.getLongitude();
		
		currLat = ((int)(currLat*100.0));
		currLat = currLat/100.0;
		
		currLong = ((int)(currLong*100.0));
		currLong = currLong/100.0;
		
		if( (currLat >= minLat) && (currLat <= maxLat) && 
				(currLong >= minLong) && (currLong <= maxLong) )
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken)
	{
		synchronized(waitLock)
		{
			numRecvd++;
			numUpdatesRecvd++;
//			System.out.println("Updates recvd "+userGUID+" time "+timeTaken
//					+" numRecvd "+numRecvd+" numSent "+numSent);
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
		
//		NUMUSERS = 100;
		
		IssueUpdates issUpd = new IssueUpdates();
		issUpd.readNomadLag();
		//issUpd.assignMobilityUserId();
		issUpd.createTransformedTrajectories();
		
		System.out.println("minLatData "+issUpd.minLatData+" maxLatData "+issUpd.maxLatData
				+" minLongData "+issUpd.minLongData+" maxLongData "+issUpd.maxLongData);
		issUpd.printLogStats();
		System.out.println("\n\n");
		issUpd.printRealUserStats();
		issUpd.runUpdates();
	}
}