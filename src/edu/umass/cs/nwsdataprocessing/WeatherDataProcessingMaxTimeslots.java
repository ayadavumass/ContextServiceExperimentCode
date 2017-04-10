package edu.umass.cs.nwsdataprocessing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.acs.geodesy.GlobalCoordinate;
import edu.umass.cs.largescalecasestudy.LargeNumUsers;

public class WeatherDataProcessingMaxTimeslots
{
	public static final long START_TIMESTAMP			= 1483228800;
	public static final long END_TIMESTAMP				= 1489568400;
	
	public static final long TIMESLOT_LEN				= 900; //15 minute timeslot.
	
	// json key constants
	public static final String PROPERTRY_KEY			= "properties";
	//public static final String ISSUED_KEY				= "ISSUED";
	public static final String ISSUED_KEY				= "INIT_ISS";
	public static final String EXPIRED_KEY				= "EXPIRED";
	public static final String PHENOM_KEY				= "PHENOM";
	public static final String GEOMETRY_KEY				= "geometry";
	public static final String TYPE_KEY					= "type";
	public static final String COORDINATE_KEY			= "coordinates";
	public static final String RADAR_INFO_KEY			= "WFO";  // stands for weather forecasting office.
	public static final String AREA_KM2_KEY				= "AREA_KM2";
	
	// json key-value constants
	public static final String POLYGON_TYPE				= "Polygon";
	public static final String MULTIPOLYGON_TYPE		= "MultiPolygon";
	
	
	public static final String WEATHER_DATA_PATH 
	    //= LargeNumUsers.WEATHER_DATA_PATH;
		= "/home/ayadav/Documents/Data/NWSWeatherData/wwa_201701010000_201703150000/1Jan15Mar2017Weather.json";
	
	private static HashMap<Long, PerDayEventStorage> perTimeSlotMap;
	
	//private static DateFormat dfm = new SimpleDateFormat("yyyyMMddHHmm");
	
	
	public static void readTheWeatherFile()
	{
		boolean featuresFound = false;
		BufferedReader br = null;
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader
					(new FileReader(WEATHER_DATA_PATH));
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				if(featuresFound)
				{
					if( sCurrentLine.startsWith("{") )
					{
						JSONObject weatherEventJSON = new JSONObject(sCurrentLine);
						JSONObject propertiesJSON = weatherEventJSON.getJSONObject(PROPERTRY_KEY);
						JSONObject geometryJSON = weatherEventJSON.getJSONObject(GEOMETRY_KEY);
						
						String WFO = propertiesJSON.getString(RADAR_INFO_KEY);
						String issueTime = propertiesJSON.getString(ISSUED_KEY);
						String expireTime = propertiesJSON.getString(EXPIRED_KEY);
						String phenomCode = propertiesJSON.getString(PHENOM_KEY);
						String areaKm2 = propertiesJSON.getString(AREA_KM2_KEY);
						
						String geometryType = geometryJSON.getString(TYPE_KEY);
						JSONArray geometryArray = geometryJSON.getJSONArray(COORDINATE_KEY);
						
						List<List<GlobalCoordinate>> polygons 
								= convertToListOfPolygons(geometryArray, geometryType);
						
//						WeatherEventStorage weatherEvents = new WeatherEventStorage
//									( WFO, issueTime, expireTime, phenomCode, 
//										getUnixTimeStamp(issueTime), getUnixTimeStamp(expireTime), 
//										polygons, Double.parseDouble(areaKm2) );
						
						long timeslot = ((getUnixTimeStamp(issueTime)-START_TIMESTAMP)/TIMESLOT_LEN)+1;
						
						
						if(perTimeSlotMap.containsKey(timeslot))
						{
							PerDayEventStorage perDayStorage 
													= perTimeSlotMap.get(timeslot);
							perDayStorage.numEvents = perDayStorage.numEvents + 1;
							perDayStorage.totalPolygonsInEvents 
													= perDayStorage.totalPolygonsInEvents+polygons.size();						
						}
						else
						{
							PerDayEventStorage perDayStorage = new PerDayEventStorage();
							perDayStorage.numEvents = perDayStorage.numEvents + 1;
							perDayStorage.totalPolygonsInEvents = perDayStorage.totalPolygonsInEvents+polygons.size();
							perDayStorage.timeslot = timeslot;
							perTimeSlotMap.put(timeslot, perDayStorage);
						}
					}
				}
				else
				{
					if(sCurrentLine.startsWith("\"features\":"))
					{
						featuresFound = true;
					}
				}
				
			}
		} catch (IOException e) 
		{
			e.printStackTrace();
		} catch (JSONException e) 
		{
			e.printStackTrace();
		} finally
		{
			try
			{
				if ( br != null )
					br.close();
			} catch ( IOException ex )
			{
				ex.printStackTrace();
			}
		}
		
		Iterator<Long> timeslotIter = perTimeSlotMap.keySet().iterator();
		List<PerDayEventStorage> perTimeSlotEventList 
									= new LinkedList<PerDayEventStorage>();
		
		while( timeslotIter.hasNext() )
		{
			long currTS 	= timeslotIter.next();
			PerDayEventStorage perDayStorage 
								= perTimeSlotMap.get(currTS);
			
			perTimeSlotEventList.add(perDayStorage);
		}
		
		perTimeSlotEventList.sort(new PerDayEventStorage());
		
		System.out.println("Printing perTimeSlotEventList");
		for(int i=0; i < perTimeSlotEventList.size(); i++)
		{
			PerDayEventStorage perdaystore = perTimeSlotEventList.get(i);
			long ts = ((perdaystore.timeslot -1 )* TIMESLOT_LEN)+START_TIMESTAMP;
			
			System.out.println("perdaystore timeslot="+perdaystore.timeslot
					+" timestamp="+ts
					+" numevents="+perdaystore.numEvents+" numploygons="
					+perdaystore.totalPolygonsInEvents);
		}
	}
	
	
	private static long getUnixTimeStamp(String timestamp)
	{
		long unixtime;
		try
		{
			DateFormat dfm = new SimpleDateFormat("yyyyMMddHHmm");
			dfm.setTimeZone(TimeZone.getTimeZone("GMT"));
			unixtime = dfm.parse(timestamp).getTime();
			unixtime=unixtime/1000;
			return unixtime;
		}
		catch (ParseException e)
		{
			e.printStackTrace();
		}
		return -1;
	}
	
	
	private static List<List<GlobalCoordinate>> convertToListOfPolygons(JSONArray polygonsArr, 
			String polygonType)
	{
		List<List<GlobalCoordinate>> polygonsList = new LinkedList<List<GlobalCoordinate>>();
		
		if( polygonType.equals(POLYGON_TYPE) )
		{
			List<GlobalCoordinate> polygon = processAPolygonFromGeoJSON( polygonsArr,  polygonType);
			//assert(polygon != null);
			if(polygon != null)
			{
				polygonsList.add(polygon);
			}
		}
		else if( polygonType.equals(MULTIPOLYGON_TYPE) )
		{
			for( int i=0; i<polygonsArr.length(); i++ )
			{
				try
				{
					JSONArray polygonsJSON = polygonsArr.getJSONArray(i);
					List<GlobalCoordinate> polygon = processAPolygonFromGeoJSON( polygonsJSON, polygonType );
					//assert(polygon != null);
					if(polygon != null)
					{
						polygonsList.add(polygon);
					}
				}
				catch (JSONException e)
				{
					e.printStackTrace();
				}
			}
		}
		else
		{
			assert(false);
		}
		return polygonsList;
	}
	
	
	private static List<GlobalCoordinate> processAPolygonFromGeoJSON( JSONArray polygonsArr, 
			String polyType )
	{
//		if( polygonsArr.length() > 1 )
//		{
//			System.out.println("More than one polygon type "+polyType+" "+polygonsArr);
//		}
//		else 
		// we always take the outer ring
		if( polygonsArr.length() >= 1 )
		{
			try
			{
				JSONArray polygonJSON = polygonsArr.getJSONArray(0);
				List<GlobalCoordinate> polygonList = new LinkedList<GlobalCoordinate>();
				
				for(int j=0; j<polygonJSON.length(); j++)
				{
					JSONArray coord = polygonJSON.getJSONArray(j);
					
					double longitude = coord.getDouble(0);
					double latitude = coord.getDouble(1);
					
					//updateBounds(latitude, longitude);
					
					GlobalCoordinate gCoord = new GlobalCoordinate(latitude, longitude);
					polygonList.add(gCoord);
				}
				
				return polygonList;
			}
			catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		else
		{
			assert(false);
		}
		return null;
	}
	
	
	public static void main( String[] args )
	{	
		perTimeSlotMap = new HashMap<Long, PerDayEventStorage>();
		readTheWeatherFile();
	}
}