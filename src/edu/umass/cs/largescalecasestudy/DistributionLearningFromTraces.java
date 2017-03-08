package edu.umass.cs.largescalecasestudy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.acs.geodesy.Ellipsoid;
import edu.umass.cs.acs.geodesy.GeodeticCalculator;
import edu.umass.cs.acs.geodesy.GeodeticCurve;
import edu.umass.cs.acs.geodesy.GlobalCoordinate;
import edu.umass.cs.acs.geodesy.GlobalPosition;


public class DistributionLearningFromTraces
{
	//public static List<UpdateRateStorage> updateStorageList;
	public static HashMap<String, PerUserDistributions> distributionsMap;
	
	private static void computeDistributionStats(String filename) throws ParseException
	{	
		PerUserDistributions perUserDist = new PerUserDistributions();
		distributionsMap.put(filename, perUserDist);
		
		BufferedReader br  = null;
		try
		{
			br = new BufferedReader(new FileReader(LargeNumUsers.USER_TRACE_DIR+"/"+filename));
			String sCurrentLine;
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				try
				{
					JSONObject entryobject = new JSONObject(sCurrentLine);
					JSONObject coordJSON = entryobject.getJSONObject(LargeNumUsers.GEO_LOC_KEY);
					JSONArray coordArray = coordJSON.getJSONArray(LargeNumUsers.COORD_KEY);
					
					double longitude 		= coordArray.getDouble(0);
					double latitude  		= coordArray.getDouble(1);
					double geoloctimestamp 	= Double.parseDouble
												(entryobject.getString(LargeNumUsers.GEOLOC_TIME));
					
					LogEntryInfo logentry   = new LogEntryInfo();
					
					logentry.latitude       = latitude;
					logentry.longitude      = longitude;
					logentry.unixtimestamp  = (long) geoloctimestamp;
					
					
					Date date = new Date(logentry.unixtimestamp*1000); // *1000 is to convert seconds to milliseconds
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // the format of your date
					sdf.setTimeZone(TimeZone.getTimeZone(LargeNumUsers.TEXAS_TIMEZONE)); // give a timezone reference for formating (see comment at the bottom
					String formattedDate = sdf.format(date);
					String onlyDay = formattedDate.split(" ")[0];
					
					//String dayStartTimestamp = onlyDay+" "+"00:00:00";
					//long diffunixtime = sdf.parse(dayStartTimestamp).getTime();  
					//diffunixtime=diffunixtime/1000;
					
					if( perUserDist.eventMapForGUID.containsKey(onlyDay) )
					{
						List<LogEntryInfo> entryList = perUserDist.eventMapForGUID.get(onlyDay);
						entryList.add(logentry);
					}
					else
					{
						List<LogEntryInfo> entryList = new LinkedList<LogEntryInfo>();
						entryList.add(logentry);
						perUserDist.eventMapForGUID.put(onlyDay, entryList);	
					}
				}
				catch (JSONException e) 
				{
					e.printStackTrace();
				}
			}
			
			// sort each list.
			Iterator<String> dayIter = perUserDist.eventMapForGUID.keySet().iterator();
			while(dayIter.hasNext())
			{
				String day = dayIter.next();
				
				List<LogEntryInfo> entrylist = perUserDist.eventMapForGUID.get(day);
				
				entrylist.sort(new LogEntryInfo());
			}
			
			dayIter = perUserDist.eventMapForGUID.keySet().iterator();
			while( dayIter.hasNext() )
			{
				String day = dayIter.next();
				List<LogEntryInfo> entrylist = perUserDist.eventMapForGUID.get(day);
				computeDistributionStatsPerDay(entrylist, perUserDist.distributionMap);
			}
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
		}
		
		System.out.println("filename="+filename+" "+perUserDist.distributionMap.size()+"\n\n");
		
		System.out.println("Priting distance");
		Iterator<Integer> iter = perUserDist.distributionMap.keySet().iterator();
		String str = "";
		while(iter.hasNext())
		{
			int numUpd = iter.next();
			DistributionInfo distInfo = perUserDist.distributionMap.get(numUpd);
			
			str=str+"numUpd="+numUpd+" , ";
			for(int i=0; i<distInfo.perUpdateDistance.size(); i++)
			{
				str= str+"["+distInfo.perUpdateDistance.get(i)
									+","+distInfo.perUpdateAngle.get(i)+"] , ";
			}
			str =str + "\n";
		}
		System.out.println(str);
		
		
		System.out.println("Priting time");
		iter = perUserDist.distributionMap.keySet().iterator();
		str  = "";
		while(iter.hasNext())
		{
			int numUpd = iter.next();
			DistributionInfo distInfo = perUserDist.distributionMap.get(numUpd);
			
			str=str+"numUpd="+numUpd+" , ";
			for(int i=0; i<distInfo.perUpdateTime.size(); i++)
			{
				str= str+((distInfo.perUpdateTime.get(i)*1.0)/(60.0*60.0))+" , ";
			}
			str =str + "\n";
		}
		System.out.println(str);
	}
	
	
	private static void computeDistributionStatsPerDay( List<LogEntryInfo> entrylist, 
			HashMap<Integer, DistributionInfo> distributionMap ) throws ParseException
	{
		assert(entrylist.size() > 0);
		
		LogEntryInfo firstlogentry  = entrylist.get(0);
		GlobalCoordinate firstcoord = new GlobalCoordinate
							(firstlogentry.latitude, firstlogentry.longitude);
		
		for( int i=1; i<entrylist.size(); i++ )
		{
			LogEntryInfo currEntry  = entrylist.get(i);
			
			GlobalCoordinate gcoord = new GlobalCoordinate
							(currEntry.latitude, currEntry.longitude);
			
			DistanceAndAngle distanceAndAngle = computeDistanceBetweenPoints(firstcoord, gcoord);
			
			int updatenum = i+1;
			
			if( distributionMap.containsKey(updatenum) )
			{
				DistributionInfo distInfo = distributionMap.get(updatenum);
				distInfo.perUpdateDistance.add(distanceAndAngle.distance);
				distInfo.perUpdateAngle.add(distanceAndAngle.angle);
			}
			else
			{
				DistributionInfo distInfo = new DistributionInfo();
				distInfo.perUpdateDistance.add(distanceAndAngle.distance);
				distInfo.perUpdateAngle.add(distanceAndAngle.angle);
				distributionMap.put(updatenum, distInfo);
			}
		}
		
		// reference date.
		Date firstdate = new Date(firstlogentry.unixtimestamp*1000); // *1000 is to convert seconds to milliseconds
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // the format of your date
		sdf.setTimeZone(TimeZone.getTimeZone(LargeNumUsers.TEXAS_TIMEZONE)); // give a timezone reference for formating (see comment at the bottom
		String formattedDate = sdf.format(firstdate);
		String onlyDay = formattedDate.split(" ")[0];
		
		String dayStartTimestamp = onlyDay+" "+"00:00:00";
		
		long midnightunixtime = sdf.parse(dayStartTimestamp).getTime();
		midnightunixtime=midnightunixtime/1000;
		
		for( int i=0; i<entrylist.size(); i++ )
		{
			if(i == 0)
			{
				LogEntryInfo logentry  = entrylist.get(i);
				long timeSincemidnight = logentry.unixtimestamp - midnightunixtime;
				
				assert(timeSincemidnight > 0);
				int updatenum = i+1;
				
				if(distributionMap.containsKey(updatenum))
				{
					DistributionInfo distInfo = distributionMap.get(updatenum);
					distInfo.perUpdateTime.add(timeSincemidnight);
				}
				else
				{
					DistributionInfo distInfo = new DistributionInfo();
					distInfo.perUpdateTime.add(timeSincemidnight);
					
					distributionMap.put(updatenum, distInfo);
				}
			}
			else
			{
				LogEntryInfo lastlogentry  = entrylist.get(i-1);
				LogEntryInfo currlogentry  = entrylist.get(i);
				
				long interEventTime = currlogentry.unixtimestamp - lastlogentry.unixtimestamp;
				if(interEventTime <= 0)
					System.out.println(currlogentry.unixtimestamp
							+" "+lastlogentry.unixtimestamp);
				
				assert(interEventTime >= 0);
				int updatenum = i+1;
				
				if(distributionMap.containsKey(updatenum))
				{
					DistributionInfo distInfo = distributionMap.get(updatenum);
					distInfo.perUpdateTime.add(interEventTime);
				}
				else
				{
					DistributionInfo distInfo = new DistributionInfo();
					distInfo.perUpdateTime.add(interEventTime);
					
					distributionMap.put(updatenum, distInfo);
				}
			}
		}
	}
	
	
	private static DistanceAndAngle computeDistanceBetweenPoints(GlobalCoordinate gcoord1, 
												GlobalCoordinate gcoord2)
	{
		Ellipsoid reference    = Ellipsoid.WGS84;
		GlobalPosition pointA  = new GlobalPosition(gcoord1.getLatitude(), 
								gcoord1.getLongitude(), 0.0); // Point A
		
		GlobalPosition userPos = new GlobalPosition(gcoord2.getLatitude(), 
								gcoord2.getLongitude(), 0.0); // Point B
		
		GeodeticCurve gcurve   = GeodeticCalculator.calculateGeodeticCurve
				(reference, userPos, pointA);
		
		double distance 	   = gcurve.getEllipsoidalDistance(); // Distance between Point A and Point B
		
		double angle = gcurve.getAzimuth();
		
		DistanceAndAngle distanceAngle = new DistanceAndAngle();
		
		distanceAngle.distance = distance;
		distanceAngle.angle = angle;
		
		return distanceAngle;
	}
	
	
	private static void computeAverageUpdateRates() throws ParseException
	{
		File folder = new File(LargeNumUsers.USER_TRACE_DIR);
		File[] listOfFiles = folder.listFiles();
		
		List<String> fileList = new LinkedList<String>();
		for (int i = 0; i < listOfFiles.length; i++)
		{
			if ( listOfFiles[i].isFile() )
			{
				String filename = listOfFiles[i].getName();

				if( filename.startsWith("TraceUser") )
				{
					//computeUpdateRateOfAUser(filename);
					//computeDistanceDistribution(filename);	
					long numLines = numberLinesInAFile(filename);
					if( numLines >= LargeNumUsers.NUM_EVENT_THRESHOLD )
					{
						fileList.add(filename);
					}
				}
				
				//filenameList.add(listOfFiles[i].getName());
				//System.out.println("File " + listOfFiles[i].getName());
			}
			else if (listOfFiles[i].isDirectory())
			{
				//assert(false);
				System.out.println("Directory " + listOfFiles[i].getName());
			}
		}
		
		System.out.println("Number of good users "+fileList.size());
		for(int i=0; i<fileList.size(); i++)
		{
			String filename = fileList.get(i);
			computeDistributionStats(filename);
		}
	}
	
	
	private static long numberLinesInAFile(String filename)
	{
		BufferedReader br = null;
		long numLines = 0;
		try
		{
			br = new BufferedReader(new FileReader(LargeNumUsers.USER_TRACE_DIR+"/"+filename));
			
			String sCurrentLine;
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				numLines++;
			}
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
		}
		finally
		{
			if(br != null)
			{
				try 
				{
					br.close();
				} 
				catch (IOException e) 
				{
					e.printStackTrace();
				}
			}
		}
		return numLines;
	}
	
	
	public static class LogEntryInfo implements Comparator<LogEntryInfo>
	{
		long unixtimestamp;
		double latitude;
		double longitude;
		
		
		@Override
		public int compare(LogEntryInfo o1, LogEntryInfo o2) 
		{
			if( o1.unixtimestamp < o2.unixtimestamp )
			{
				return -1;
			}
			else
			{
				return 1;
			}
		}
	}
	
	
	public static class UserPerDayUpdateStorage
	{
		String filename;
		long numEvents;
		long starttimestamp;
		long endtimestamp;

		public UserPerDayUpdateStorage()
		{
			filename 		= "";
			numEvents 		= 0;
			starttimestamp 	= -1;
			endtimestamp    = -1;
		}
	}
	
	public static class UpdateRateStorage implements Comparator<UpdateRateStorage>
	{
		String filename;
		double avgUpdateRate;
		double minUpdateRate;
		double maxUpdateRate;
		long avgStimestamp;
		long avdEtimestamp;
		int numActiveDays;

		public UpdateRateStorage()
		{
			filename = "";
			avgUpdateRate = -1;
			minUpdateRate = -1;
			maxUpdateRate = -1;
			avgStimestamp = -1;
			avdEtimestamp = -1;
			numActiveDays = -1;
		}
		
		@Override
		public int compare(UpdateRateStorage o1, UpdateRateStorage o2) 
		{
			if(o1.avgUpdateRate < o2.avgUpdateRate)
			{
				return -1;
			}
			else
			{
				return 1;
			}
		}
	}
	
	public static class DistributionInfo
	{
		List<Long> perUpdateTime;
		List<Double> perUpdateDistance;
		List<Double> perUpdateAngle;
		
		public DistributionInfo()
		{
			perUpdateTime = new LinkedList<Long>();
			perUpdateDistance = new LinkedList<Double>();
			perUpdateAngle = new LinkedList<Double>();
		}
	}
	
	
	public static class PerUserDistributions
	{
		// filename of the corresponding user.
		public String filename;
		// update is the update num
		public HashMap<Integer, DistributionInfo> distributionMap;
		// string in the key is the date.
		public HashMap<String, List<LogEntryInfo>> eventMapForGUID;
		
		public PerUserDistributions()
		{
			distributionMap = new HashMap<Integer, DistributionInfo>();
			eventMapForGUID = new HashMap<String, List<LogEntryInfo>>();
		}
	}
	
	
	public static int getNumUpdatesFromDistForUser(String filename)
	{
		PerUserDistributions perUserDist = distributionsMap.get(filename);
		
		List<Integer> updatesPerDay = new LinkedList<Integer>();
		
		Iterator<String> dateIter = perUserDist.eventMapForGUID.keySet().iterator();
		
		while( dateIter.hasNext() )
		{
			List<LogEntryInfo> logEntryList = perUserDist.eventMapForGUID.get(dateIter.next());
			
			// size is the number of updates on that day.
			updatesPerDay.add(logEntryList.size());
		}
		int randIndex = LargeNumUsers.distibutionRand.nextInt(updatesPerDay.size());
		
		return updatesPerDay.get(randIndex);
	}
	
	
	public static long getNextUpdateTimeFromDist(String filename, int updatenum)
	{
		PerUserDistributions perUserDist  = distributionsMap.get(filename);
		
		DistributionInfo updateNumDistInf = perUserDist.distributionMap.get(updatenum);
		
		if(updateNumDistInf != null)
		{
			assert(updateNumDistInf.perUpdateTime.size() > 0);
			
			int randIndex = LargeNumUsers.distibutionRand.nextInt
							(updateNumDistInf.perUpdateTime.size());
			
			long timeInSecs = updateNumDistInf.perUpdateTime.get(randIndex);
			return timeInSecs;
		}
		else
		{
			System.out.println("No time distribution found for current updatenum "+updatenum);
			return -1;
		}
	}
	
	
	public static DistanceAndAngle getDistAngleFromDist(String filename, int updatenum)
	{
		PerUserDistributions perUserDist  = distributionsMap.get(filename);
		DistributionInfo updateNumDistInf = perUserDist.distributionMap.get(updatenum);
		
		if(updateNumDistInf != null)
		{
			assert(updateNumDistInf.perUpdateDistance.size() > 0);
			
			assert(updateNumDistInf.perUpdateDistance.size() 
							== updateNumDistInf.perUpdateAngle.size());
			
			int randIndex = LargeNumUsers.distibutionRand.nextInt
							(updateNumDistInf.perUpdateDistance.size());
			
			double distance = updateNumDistInf.perUpdateDistance.get(randIndex);
			double angle = updateNumDistInf.perUpdateAngle.get(randIndex);
			
			DistanceAndAngle distAngleObj = new DistanceAndAngle();
			distAngleObj.distance = distance;
			distAngleObj.angle = angle;
			return distAngleObj;
			//return timeInSecs;
		}
		else
		{
			System.out.println("No time distribution found for current updatenum "+updatenum);
			return null;
		}
	}
	
	public static class DistanceAndAngle
	{
		double distance;
		double angle;
	}
	
	
	public static void main(String[] args) throws ParseException
	{
		distributionsMap = new HashMap<String, PerUserDistributions>();
		
		//updateStorageList = new LinkedList<UpdateRateStorage>();
		
		computeAverageUpdateRates();
		//updateStorageList.sort(new UpdateRateStorage());
		//System.out.println("Number of useful users "+updateStorageList.size());
		//double perc = 0.0;
		/*for(int i=0; i<updateStorageList.size(); i++)
		{
			perc = ((i+1)*1.0)/updateStorageList.size();
			UpdateRateStorage upd = updateStorageList.get(i);

			//Date sdate = new Date(upd.avgStimestamp*1000); // *1000 is to convert seconds to milliseconds
			//SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // the format of your date
			//sdf.setTimeZone(TimeZone.getTimeZone(TEXAS_TIMEZONE)); // give a timezone reference for formating (see comment at the bottom
			//String sDateString = sdf.format(sdate);
			//
			//Date edate = new Date(upd.avdEtimestamp*1000); // *1000 is to convert seconds to milliseconds
			//String eDateString = sdf.format(edate);
			
			double shour = (upd.avgStimestamp*1.0)/(60.0*60.0);
			double ehour = (upd.avdEtimestamp*1.0)/(60.0*60.0);
			
			System.out.println(upd.filename+" , "+(perc)+" , "+upd.avgUpdateRate
					+" , "+upd.minUpdateRate
					+" , "+upd.maxUpdateRate
					+" , "+upd.numActiveDays
					+" , "+shour
					+" , "+ehour);
		}*/
	}
	
	
	/*private static void computeUpdateRateOfAUser(String filename) throws ParseException
	{
		BufferedReader br  = null;
		long totalEvents   = 0;
		
		HashMap<String, UserPerDayUpdateStorage> userPerDayMap 
						   = new HashMap<String, UserPerDayUpdateStorage>();
		
		try
		{
			br = new BufferedReader(new FileReader(USER_TRACE_DIRECTORY+"/"+filename));
			String sCurrentLine;

			while( (sCurrentLine = br.readLine()) != null )
			{
				try
				{
					JSONObject jsonObject = new JSONObject(sCurrentLine);
					double timestamp = Double.parseDouble(jsonObject.getString(GEOLOC_TIME));
					long unixtimeinsec = (long)timestamp;
					
					
					Date date = new Date(unixtimeinsec*1000); // *1000 is to convert seconds to milliseconds
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // the format of your date
					sdf.setTimeZone(TimeZone.getTimeZone(TEXAS_TIMEZONE)); // give a timezone reference for formating (see comment at the bottom
					String formattedDate = sdf.format(date);
					String onlyDay = formattedDate.split(" ")[0];

					String dayStartTimestamp = onlyDay+" "+"00:00:00";
					long diffunixtime = sdf.parse(dayStartTimestamp).getTime();  
					diffunixtime=diffunixtime/1000;


					UserPerDayUpdateStorage userPerDayObj = userPerDayMap.get(onlyDay);

					if(userPerDayObj == null)
					{
						userPerDayObj = new UserPerDayUpdateStorage();
						userPerDayObj.filename = filename;
						userPerDayObj.numEvents++;
						userPerDayObj.starttimestamp = unixtimeinsec - diffunixtime;
						userPerDayObj.endtimestamp = unixtimeinsec - diffunixtime;

						assert(userPerDayObj.endtimestamp >= 0);
						assert(userPerDayObj.starttimestamp >= 0);
						assert(userPerDayObj.endtimestamp >= userPerDayObj.starttimestamp);
						
						userPerDayMap.put(onlyDay, userPerDayObj);
					}
					else
					{
						userPerDayObj.numEvents++;
						if( (unixtimeinsec-diffunixtime) < userPerDayObj.starttimestamp)
								userPerDayObj.starttimestamp = (unixtimeinsec-diffunixtime);

						if( (unixtimeinsec-diffunixtime) > userPerDayObj.endtimestamp )
							userPerDayObj.endtimestamp = unixtimeinsec-diffunixtime;

						assert(userPerDayObj.endtimestamp >= 0);
						assert(userPerDayObj.starttimestamp >= 0);

						assert(userPerDayObj.endtimestamp >= userPerDayObj.starttimestamp);
					}		
					totalEvents++;
				}
				catch (JSONException e)
				{
					e.printStackTrace();
				}
			}
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
		}
		
		
		if( (totalEvents >= NUM_EVENT_THRESHOLD)  && (userPerDayMap.size() > 0) )
		{
			// key is the number of events, value is the number of times 
			HashMap<Long, Long> numEventsMap = new HashMap<Long, Long>();
			
			
			Iterator<String> dateIter = userPerDayMap.keySet().iterator();
			
			double sumAvgUpdateRate = 0;
			double minUpdateRate = -1;
			double maxUpdateRate = -1;
			double sumMinTime = 0;
			double sumMaxTime = 0;
			int actualdays = 0;
			
			while( dateIter.hasNext() )
			{
				String day = dateIter.next();
				UserPerDayUpdateStorage userPerDayObj = userPerDayMap.get(day);

				if(userPerDayObj.endtimestamp >= userPerDayObj.starttimestamp)
				{
					actualdays++;
					//double updateRate = (userPerDayObj.numEvents*60.0*60.0)/(userPerDayObj.endtimestamp-userPerDayObj.starttimestamp);
					double updateRate = userPerDayObj.numEvents;
					sumAvgUpdateRate = sumAvgUpdateRate + updateRate;

					if( (minUpdateRate == -1) || (updateRate < minUpdateRate ) )
					{
						minUpdateRate = updateRate;
					}

					if( (maxUpdateRate == -1) || (updateRate > maxUpdateRate ) )
					{
						maxUpdateRate = updateRate;
					}

					sumMinTime = sumMinTime + userPerDayObj.starttimestamp;
					sumMaxTime = sumMaxTime + userPerDayObj.endtimestamp;
				}
				
				if( numEventsMap.containsKey(userPerDayObj.numEvents) )
				{
					long numOcc = numEventsMap.get(userPerDayObj.numEvents);
					numOcc++;
					
					numEventsMap.put(userPerDayObj.numEvents, numOcc);
				}
				else
				{
					numEventsMap.put(userPerDayObj.numEvents, (long)1);
				}
			}

			double avgUpdateRate = sumAvgUpdateRate/actualdays;
			double avgMinTime = sumMinTime/actualdays;
			double avgMaxTime = sumMaxTime/actualdays;
			

			UpdateRateStorage upd = new UpdateRateStorage();
			upd.avgUpdateRate = avgUpdateRate;
			upd.filename = filename;
			upd.avgStimestamp = (long)avgMinTime;
			upd.avdEtimestamp = (long)avgMaxTime;
			upd.minUpdateRate = minUpdateRate;
			upd.maxUpdateRate = maxUpdateRate;
			upd.numActiveDays = actualdays;
			
			updateStorageList.add(upd);
			
			
			System.out.println("For filename "+filename);
			
			
			Iterator<Long> numEventIter = numEventsMap.keySet().iterator();
			
			while(numEventIter.hasNext())
			{
				long numEvents = numEventIter.next();
				long numOccurs = numEventsMap.get(numEvents);
				System.out.println("numEvents="+numEvents+", numOccurs="+numOccurs);
			}	
			System.out.println("\n\n");
		}
	}*/
}