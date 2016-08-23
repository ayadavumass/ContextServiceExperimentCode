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

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.nomadlogProcessing.LogEntryClass;
import edu.umass.cs.utils.UtilFunctions;

public class IssueUpdates extends AbstractRequestSendingClass
{
	public static final double UPD_LOSS_TOLERANCE       = 0.0;
	public static final int NUMUSERS					= 10000;
	
	public static final int WAIT_TIME					= 100000;
	
	public static final double minBuffaloLat 			= 42.0;
	public static final double maxBuffaloLat 			= 44.0;
	
	public static final double minBuffaloLong			= -80.0;
	public static final double maxBuffaloLong 			= -78.0;
	
	public static final double timeContractionFactor 	= 2976.569444444;
	
	public static final double MIN_UNIX_TIME			= 1385770103;
	public static final double MAX_UNIX_TIME			= 1391127928;
	
	public static final String guidPrefix				= "GUID";
	
	public static final String latitudeAttr				= "latitude";
	public static final String longitudeAttr			= "longitude";
	
	
	public static ContextServiceClient<String> csClient;
	public static boolean useGNS						= false;
	
	public static final String nomadLogDataPath 		
									= "/users/ayadavum/nomadLog/loc_seq";
	
	private HashMap<Integer, List<LogEntryClass>> userMobilityEntryHashMap;
	
	
	private HashMap<Integer, Integer> lastEntrySentMap;
	
	private HashMap<Integer, Integer> realIDToMobilityIdMap;
	
	private Random rand;
	
	private double simulatedTime;
	
	private long requestId;
	
	private long numUpdatesRecvd;
	private double sumUpdateLatency;
	
	private static String csHost;
	private static int csPort;
	
	
	public IssueUpdates() throws NoSuchAlgorithmException, IOException
	{
		super( UPD_LOSS_TOLERANCE );
		userMobilityEntryHashMap = new HashMap<Integer, List<LogEntryClass>>();
		realIDToMobilityIdMap    = new HashMap<Integer, Integer>();
		lastEntrySentMap         = new HashMap<Integer, Integer>();
		
		csClient  = new ContextServiceClient<String>(csHost, csPort, 
						ContextServiceClient.HYPERSPACE_BASED_CS_TRANSFORM);
		
		rand = new Random();
	}
	
	private void readNomadLag() throws IOException
	{
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
				
				LogEntryClass logEntryObj = new LogEntryClass(unixtimestamp, 
						latitude, longitude);
				
				List<LogEntryClass> userEventList = userMobilityEntryHashMap.get(userId);
				if( userEventList == null )
				{
					userEventList = new LinkedList<LogEntryClass>();
					userEventList.add(logEntryObj);
					userMobilityEntryHashMap.put(userId, userEventList);
				}
				else
				{
					userEventList.add(logEntryObj);
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
			
			List<LogEntryClass> logEntryList 
							= userMobilityEntryHashMap.get(userId);
			
			logEntryList.sort
			((o1, o2) -> o1.getUnixTimeStamp().compareTo(o2.getUnixTimeStamp()));
		}
	}
	
	private void assignMobilityUserId()
	{
		for( int i=0; i<NUMUSERS; i++ )
		{
			int randMobId = rand.nextInt( userMobilityEntryHashMap.size() );
			realIDToMobilityIdMap.put(i, randMobId);
			lastEntrySentMap.put(i, -1);
		}
	}
	
	private void runUpdates() throws InterruptedException
	{
		simulatedTime = MIN_UNIX_TIME;
		while( simulatedTime <= MAX_UNIX_TIME )
		{
			Thread.sleep(1000);
			simulatedTime = simulatedTime +timeContractionFactor;
			sendUpdatesWhoseTimeHasCome();
		}
	}
	
	private void sendUpdatesWhoseTimeHasCome()
	{
		Iterator<Integer> userIdIter = realIDToMobilityIdMap.keySet().iterator();
		while( userIdIter.hasNext() )
		{
			int realId = userIdIter.next();
			int mobilityId = realIDToMobilityIdMap.get(realId);
			
			System.out.println("realId "+realId+" mobilityId "+mobilityId);
			
			List<LogEntryClass> logEntryList = userMobilityEntryHashMap.get(mobilityId);
			
			System.out.println("logEntryList "+logEntryList.size());
			
			int lastIndex = lastEntrySentMap.get(realId);
			lastIndex++;
			
			while( lastIndex < logEntryList.size() )
			{
				LogEntryClass logEntry = logEntryList.get(lastIndex);
				sendUpdate(realId, logEntry );
			}
		}
	}
	
	private void sendUpdate(int realId, LogEntryClass logEntry )
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
		
		//int randomAttrNum = updateRand.nextInt(SearchAndUpdateDriver.numAttrs);
		//double randVal = SearchAndUpdateDriver.ATTR_MIN 
		//		+updateRand.nextDouble()*(SearchAndUpdateDriver.ATTR_MAX - SearchAndUpdateDriver.ATTR_MIN);
		
		JSONObject attrValJSON = new JSONObject();
		try
		{
			attrValJSON.put(latitudeAttr, logEntry.getLatitude());
			attrValJSON.put(longitudeAttr, logEntry.getLongitude());
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		
		ExperimentUpdateReply updateRep = new ExperimentUpdateReply
													(requestId++, userGUID);
		
		csClient.sendUpdateWithCallBack(userGUID, null, 
										attrValJSON, -1, updateRep, this.getCallBack());
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
				throws NoSuchAlgorithmException, IOException, InterruptedException
	{
		csHost = args[0];
		csPort = Integer.parseInt(args[1]);
		IssueUpdates issUpd = new IssueUpdates();
		issUpd.readNomadLag();
		issUpd.assignMobilityUserId();
		issUpd.runUpdates();
	}
}