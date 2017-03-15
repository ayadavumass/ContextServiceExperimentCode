package edu.umass.cs.confidentialdataprocessing;

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


public class ComputeUpdateUserRatesPerDayMethodGNSLogs
{
	public static final String USER_TRACE_DIRECTORY 
													= "/home/ayadav/Documents/Data/confidentialUserTraces/removeDupGNSLogs";
	
	public static final String TEXAS_TIMEZONE		= "GMT-6";
	//"geoLocationCurrentTimestamp"
	public static final String GEOLOC_TIME			= "geoLocationCurrentTimestamp";
	public static final String USERNAME_KEY			= "username";
	public static final String GEOLOC_KEY			= "geoLocationCurrent";
	public static final String COORD_KEY			= "coordinates";
	
	
	public static final int NUM_EVENT_THRESHOLD		= 5;
	public static final int NUM_DAY_THRESHOLD		= 2;
	
	public static List<UpdateRateStorage> updateStorageList;
	
	public static HashMap<String, Boolean> usernameMap;
	
	private static void computeUpdateRateOfAUser(String filename) throws ParseException
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
					
					if( jsonObject.has(USERNAME_KEY) )
					{
						String username = jsonObject.getString(USERNAME_KEY);
						if( username.contains("goerkem") || username.contains("westy")
								|| username.contains("gns.name") || username.contains("ayadav") )
						{
							return;
						}
						else
						{
							usernameMap.put(username, true);
						}
					}
					
					double timestamp = Double.parseDouble(jsonObject.getString(GEOLOC_TIME));	
					long unixtimeinsec = (long)timestamp;
					
					
					Date date = new Date(unixtimeinsec*1000); // *1000 is to convert seconds to milliseconds
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // the format of your date
					sdf.setTimeZone(TimeZone.getTimeZone(TEXAS_TIMEZONE)); // give a timezone reference for formating (see comment at the bottom
					String formattedDate = sdf.format(date);
					String onlyDay = formattedDate.split(" ")[0];
					
					String dayStartTimestamp = onlyDay+" "+"00:00:00";
					long diffunixtime = sdf.parse(dayStartTimestamp).getTime();
					diffunixtime = diffunixtime/1000;
					
					if(!jsonObject.has(GEOLOC_KEY))
						System.out.println("Not found "+jsonObject);
					
					JSONObject coordJSON = new JSONObject(jsonObject.getString(GEOLOC_KEY));
					JSONArray coordArray = coordJSON.getJSONArray(COORD_KEY);
					
					GlobalCoordinate currCoord = new GlobalCoordinate
												(coordArray.getDouble(1), coordArray.getDouble(0));
					
					UserPerDayUpdateStorage userPerDayObj = userPerDayMap.get(onlyDay);
					
					if(userPerDayObj == null)
					{
						userPerDayObj = new UserPerDayUpdateStorage();
						userPerDayObj.filename = filename;
						userPerDayObj.numEvents++;
						userPerDayObj.starttimestamp = unixtimeinsec - diffunixtime;
						userPerDayObj.endtimestamp = unixtimeinsec - diffunixtime;
						userPerDayObj.lastCoord = currCoord;
						
						
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
						
						double dist =computeDistanceBetweenPoints(userPerDayObj.lastCoord, 
																					currCoord);
						
						userPerDayObj.totalDistance = userPerDayObj.totalDistance + dist;
						
						userPerDayObj.lastCoord = currCoord;
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
		
		
		//if( (totalEvents >= NUM_EVENT_THRESHOLD)  && (userPerDayMap.size() > 0) )
		if( (userPerDayMap.size() >= NUM_DAY_THRESHOLD) && (totalEvents >= NUM_EVENT_THRESHOLD) )
		{
			Iterator<String> dateIter = userPerDayMap.keySet().iterator();
			
			double sumAvgUpdateRate = 0;
			double minUpdateRate = -1;
			double maxUpdateRate = -1;
			double sumMinTime = 0;
			double sumMaxTime = 0;
			int actualdays = 0;
			
			double minDist = -1;
			double maxDist = -1;
			double sumDist = 0;
			
			while( dateIter.hasNext() )
			{
				String day = dateIter.next();
				UserPerDayUpdateStorage userPerDayObj = userPerDayMap.get(day);
				
				if(userPerDayObj.endtimestamp >= userPerDayObj.starttimestamp)
				{
					actualdays++;
					//double updateRate = (userPerDayObj.numEvents*60.0*60.0)/(userPerDayObj.endtimestamp-userPerDayObj.starttimestamp);
					double updateRate = userPerDayObj.numEvents;
					double totalDist = userPerDayObj.totalDistance;
					
					sumAvgUpdateRate = sumAvgUpdateRate + updateRate;
					
					if( (minUpdateRate == -1) || (updateRate < minUpdateRate ) )
					{
						minUpdateRate = updateRate;
					}
					
					if( (maxUpdateRate == -1) || (updateRate > maxUpdateRate ) )
					{
						maxUpdateRate = updateRate;
					}
					
					if( (minDist == -1) || (totalDist < minDist ) )
					{
						minDist = totalDist;
					}
					
					if( (maxDist == -1) || (totalDist > maxDist ) )
					{
						maxDist = totalDist;
					}
					
					sumMinTime = sumMinTime + userPerDayObj.starttimestamp;
					sumMaxTime = sumMaxTime + userPerDayObj.endtimestamp;
					sumDist = sumDist + totalDist;
				}
			}
			
			double avgUpdateRate = sumAvgUpdateRate/actualdays;
			double avgMinTime = sumMinTime/actualdays;
			double avgMaxTime = sumMaxTime/actualdays;
			double avgDist = sumDist/actualdays;
			
			UpdateRateStorage upd = new UpdateRateStorage();
			upd.avgUpdateRate = avgUpdateRate;
			upd.filename = filename;
			upd.avgStimestamp = (long)avgMinTime;
			upd.avgEtimestamp = (long)avgMaxTime;
			upd.minUpdateRate = minUpdateRate;
			upd.maxUpdateRate = maxUpdateRate;
			upd.numActiveDays = actualdays;
			
			upd.avgTotalDistance = avgDist;
			upd.minTotalDistance = minDist;
			upd.maxTotalDistance = maxDist;
			
			updateStorageList.add(upd);
		}
	}
	
	private static double computeDistanceBetweenPoints(GlobalCoordinate gcoord1, 
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

		//double angle = gcurve.getAzimuth();
		return distance;
	}
	
	private static void computeAverageUpdateRates() throws ParseException
	{
		File folder = new File(USER_TRACE_DIRECTORY);
		File[] listOfFiles = folder.listFiles();
		
		for (int i = 0; i < listOfFiles.length; i++)
		{
			if ( listOfFiles[i].isFile() )
			{
				String filename = listOfFiles[i].getName();
				
				if( filename.startsWith("GNSLogUserNum") )
				{
					computeUpdateRateOfAUser(filename);
				}
			}
			else if (listOfFiles[i].isDirectory())
			{
				//assert(false);
				System.out.println("Directory " + listOfFiles[i].getName());
		    }
		}
	}
	
	public static class UserPerDayUpdateStorage
	{
		String filename;
		long numEvents;
		long starttimestamp;
		long endtimestamp;
		GlobalCoordinate lastCoord;
		// in meters
		double totalDistance;
		
		public UserPerDayUpdateStorage()
		{
			filename 		= "";
			numEvents 		= 0;
			starttimestamp 	= -1;
			endtimestamp    = -1;
			lastCoord       = null;
			totalDistance	= 0.0;
		}
	}
	
	
	public static class UpdateRateStorage implements Comparator<UpdateRateStorage>
	{
		public static final int AVG_UPDATE_SORTING		= 1;
		public static final int MIN_UPDATE_SORTING		= 2;
		public static final int MAX_UPDATE_SORTING		= 3;
		
		public static final int AVG_DIST_SORTING		= 4;
		public static final int MIN_DIST_SORTING		= 5;
		public static final int MAX_DIST_SORTING		= 6;
		
		
		String filename;
		double avgUpdateRate;
		double minUpdateRate;
		double maxUpdateRate;
		long avgStimestamp;
		long avgEtimestamp;
		int numActiveDays;
		int sorttype;
		
		double avgTotalDistance;
		double minTotalDistance;
		double maxTotalDistance;
		
		
		public UpdateRateStorage()
		{
			filename = "";
			avgUpdateRate 	 = -1;
			minUpdateRate 	 = -1;
			maxUpdateRate 	 = -1;
			avgStimestamp 	 = -1;
			avgEtimestamp 	 = -1;
			numActiveDays 	 = -1;
			avgTotalDistance = -1;
			minTotalDistance = -1;
			
		}
		
		public UpdateRateStorage(int sorttype)
		{
			this.sorttype = sorttype;
		}
		
		@Override
		public int compare(UpdateRateStorage o1, UpdateRateStorage o2) 
		{
			switch(sorttype)
			{
				case AVG_UPDATE_SORTING:
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
				case MIN_UPDATE_SORTING:
				{
					if(o1.minUpdateRate < o2.minUpdateRate)
					{
						return -1;
					}
					else
					{
						return 1;
					}
				}
				case MAX_UPDATE_SORTING:
				{
					if(o1.maxUpdateRate < o2.maxUpdateRate)
					{
						return -1;
					}
					else
					{
						return 1;
					}
				}
				case AVG_DIST_SORTING:
				{
					if(o1.avgTotalDistance < o2.avgTotalDistance)
					{
						return -1;
					}
					else
					{
						return 1;
					}
				}
				case MIN_DIST_SORTING:
				{
					if(o1.minTotalDistance < o2.minTotalDistance)
					{
						return -1;
					}
					else
					{
						return 1;
					}
				}
				case MAX_DIST_SORTING:
				{
					if(o1.maxTotalDistance < o2.maxTotalDistance)
					{
						return -1;
					}
					else
					{
						return 1;
					}
				}
				default:
				{
					assert(false);
					return -1;
				}
			}
		}
	}
	
	
	public static void main(String[] args) throws ParseException
	{
		usernameMap = new HashMap<String, Boolean>();
		updateStorageList = new LinkedList<UpdateRateStorage>();
		computeAverageUpdateRates();
		updateStorageList.sort(new UpdateRateStorage(UpdateRateStorage.MAX_DIST_SORTING));
		
		System.out.println("Number of useful users "+updateStorageList.size());
		
		double perc = 0.0;
		double sumUpdateRate = 0.0;
		
		for(int i=0; i<updateStorageList.size(); i++)
		{
			perc = ((i+1)*1.0)/updateStorageList.size();
			UpdateRateStorage upd = updateStorageList.get(i);
			
//			Date sdate = new Date(upd.avgStimestamp*1000); // *1000 is to convert seconds to milliseconds
//			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // the format of your date
//			sdf.setTimeZone(TimeZone.getTimeZone(TEXAS_TIMEZONE)); // give a timezone reference for formating (see comment at the bottom
//			String sDateString = sdf.format(sdate);
//			
//			Date edate = new Date(upd.avdEtimestamp*1000); // *1000 is to convert seconds to milliseconds
//			String eDateString = sdf.format(edate);
			
			double shour = (upd.avgStimestamp*1.0)/(60.0*60.0);
			double ehour = (upd.avgEtimestamp*1.0)/(60.0*60.0);
			
			System.out.println(upd.filename+" , "+upd.numActiveDays
					+" , "+(perc)
					+" , "+upd.avgUpdateRate
					+" , "+upd.minUpdateRate
					+" , "+upd.maxUpdateRate
					+" , "+upd.avgTotalDistance
					+" , "+upd.minTotalDistance
					+" , "+upd.maxTotalDistance
					+" , "+shour
					+" , "+ehour);
			
			sumUpdateRate = sumUpdateRate + upd.avgUpdateRate;
		}
		
		System.out.println("sumUpdateRate="+(sumUpdateRate/updateStorageList.size()));
		
		System.out.println("username size "+usernameMap.size());
		Iterator<String> userNameIter = usernameMap.keySet().iterator();
		while(userNameIter.hasNext())
		{
			String username = userNameIter.next();
			System.out.println("username "+username);
		}
	}
}