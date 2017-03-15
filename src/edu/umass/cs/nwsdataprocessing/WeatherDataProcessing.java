package edu.umass.cs.nwsdataprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.acs.geodesy.GlobalCoordinate;

public class WeatherDataProcessing
{
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
	
	///home/adipc/Downloads/weatherdata/wwa_201311300000_201401310000
	//public static final String weatherDataPath 			
	//		= "/home/adipc/Downloads/weatherdata/wwa_201311300000_201401310000/weatherEvent.json";
	
	public static final String WEATHER_DATA_PATH 		= "/home/ayadav/Documents/Data/weatherdata/wwa_201512010000_201602280000/Dec2015Feb2016WeatherDataForUS.geojson";
	
	private final List<WeatherEventStorage> weatherEventsObjectList;
	private final HashMap<String, PerDayEventStorage> perDayStorageMap;
	private final List<Long> activeTimeList;
	
	
	private DateFormat dfm = new SimpleDateFormat("yyyyMMddHHmm");
	
	//private double sumActiveTime						= 0.0;
	
//	private double minimumLat							= 60;
//	private double maximumLat							= 0;
//	
//	
//	private double minimumLong							= 0;
//	private double maximumLong							= -300;
	
	
	public WeatherDataProcessing()
	{
		weatherEventsObjectList = new LinkedList<WeatherEventStorage>();
		perDayStorageMap = new HashMap<String, PerDayEventStorage>();
		activeTimeList = new LinkedList<Long>();
		readTheWeatherFile();
		//filterBuffaloWeatherEvents();
		
		//dfm.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
	public List<WeatherEventStorage> getWeatherObjectList()
	{
		return weatherEventsObjectList;
	}
	
	public void readTheWeatherFile()
	{
		long lateWarnings = 0;
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
						
						WeatherEventStorage weatherEvents = new WeatherEventStorage
									( WFO, issueTime, expireTime, phenomCode, 
										getUnixTimeStamp(issueTime), getUnixTimeStamp(expireTime), 
										polygons, Double.parseDouble(areaKm2) );
						
						
						long activeTime = getUnixTimeStamp(expireTime) - getUnixTimeStamp(issueTime);
						
						if (activeTime < 0)
							lateWarnings++;
						else
							activeTimeList.add(activeTime);
						
						//weatherEventsObjectList.add(weatherEvents);
						String date = issueTime.substring(0, 8);
						
						if(perDayStorageMap.containsKey(date))
						{
							PerDayEventStorage perDayStorage 
													= perDayStorageMap.get(date);
							perDayStorage.numEvents = perDayStorage.numEvents + 1;
							perDayStorage.totalPolygonsInEvents 
													= perDayStorage.totalPolygonsInEvents+polygons.size();						
						}
						else
						{
							PerDayEventStorage perDayStorage = new PerDayEventStorage();
							perDayStorage.numEvents = perDayStorage.numEvents + 1;
							perDayStorage.totalPolygonsInEvents = perDayStorage.totalPolygonsInEvents+polygons.size();
							perDayStorageMap.put(date, perDayStorage);							
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
		
		
		// print per day map
		
		Iterator<String> dateIter = perDayStorageMap.keySet().iterator();
		List<Long> perDayEventList = new LinkedList<Long>();
		List<Long> totalPolygonsList = new LinkedList<Long>();
		
		while(dateIter.hasNext())
		{
			String currDate 	= dateIter.next();
			PerDayEventStorage perDayStorage 
								= perDayStorageMap.get(currDate);
			
			perDayEventList.add(perDayStorage.numEvents);
			totalPolygonsList.add(perDayStorage.totalPolygonsInEvents);
			
			//System.out.println("currDate "+currDate+" numEvents "
			//								+ perDayStorage.numEvents
			//								+ " totalPolygonsInEvents "
			//								+ perDayStorage.totalPolygonsInEvents);
		}
		
		perDayEventList.sort(null);
		totalPolygonsList.sort(null);
		
		System.out.println("Printing perDayEventList");
		for(int i=0; i < perDayEventList.size(); i++)
		{
			double cdfperc = ((i+1)*1.0)/perDayEventList.size();
			System.out.println(cdfperc+","+perDayEventList.get(i));
		}
		
		System.out.println("Printing totalPolygonsList");
		for(int i=0; i < totalPolygonsList.size(); i++)
		{
			double cdfperc = ((i+1)*1.0)/totalPolygonsList.size();
			System.out.println(cdfperc+","+totalPolygonsList.get(i));
		}
		System.out.println("Number of late warnings "+lateWarnings);
		writeActiveTimeCDFFile();
		
	}
	
	
	private void writeActiveTimeCDFFile()
	{
		activeTimeList.sort(null);
		
		BufferedWriter bw = null;
		FileWriter fw = null;
		
		try 
		{
			
			fw = new FileWriter("AtiveTimeCDF.csv");
			bw = new BufferedWriter(fw);
			
			for(int i=0; i<activeTimeList.size(); i++)
			{
				double cdfperc = ((i+1)*1.0)/activeTimeList.size();
				String str = cdfperc+","+activeTimeList.get(i)+"\n";
				
				bw.write(str);
			}
			
		} catch (IOException e) 
		{
			e.printStackTrace();
		} finally 
		{
			try
			{
				if (bw != null)
					bw.close();
				
				if (fw != null)
					fw.close();
			}
			catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
		
	}
	
	public void printAreaBounds()
	{
//		System.out.println("Min Latitude "+minimumLat+" Max Latitude "+maximumLat
//				+" Min Longitude "+minimumLong+" Max Longitude "+maximumLong);
	}
	
	/*private void filterBuffaloWeatherEvents()
	{
		long weatherEventId = 0;
		for( int i=0; i < weatherEventList.size(); i++ )
		{
			JSONObject weatherEventJSON = weatherEventList.get(i);
			try
			{
				// propertiesKey
				JSONObject propJSON = weatherEventJSON.getJSONObject(propertiesKey);
				String issuedTime = propJSON.getString(ISSUED_Key);
				String expiredTime = propJSON.getString(EXPIREDKey);
				String weatherPheCode = propJSON.getString(PHENOMKey);
				JSONObject geoJSON = weatherEventJSON.getJSONObject(geometryKey);
				String polygonType = geoJSON.getString(typeKey);
				JSONArray polygonsArr = geoJSON.getJSONArray(polygonKey);
				
				long issuedUnixTime = getUnixTimeStamp(issuedTime);
				long finalUnixTime  = getUnixTimeStamp(expiredTime);
				
				List<List<GlobalCoordinate>> buffaloWeatherEvents 
										= convertToListOfPolygons(polygonsArr, polygonType);
				
				if( buffaloWeatherEvents.size() > 0 )
				{
					
					//long issuedUnixTime = getUnixTimeStamp(issuedTime);
					//long finalUnixTime  = getUnixTimeStamp(expiredTime);
					WeatherEventStorage weatherEvent 
						= new WeatherEventStorage(weatherEventId++, 
								issuedTime, expiredTime, weatherPheCode, 
								buffaloWeatherEvents , issuedUnixTime,  finalUnixTime );
					
					
					buffaloAreaWeatherEvents.add(weatherEvent);
					dfm.setTimeZone(TimeZone.getTimeZone("GMT"));
					
					
					long expiryTimeLong;
					try 
					{
						expiryTimeLong = dfm.parse(expiredTime).getTime();
						
						expiryTimeLong=expiryTimeLong/1000;
						
						this.sumActiveTime = this.sumActiveTime 
											+(expiryTimeLong-issuedUnixTime );
					} 
					catch (ParseException e) 
					{
						e.printStackTrace();
					}
				}
			}
			catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		System.out.println("weatherEventList "+weatherEventList.size()
							+" buffaloAreaWeatherEvents "+buffaloAreaWeatherEvents.size() );
		
		// sort the list according to issue timestamp
		buffaloAreaWeatherEvents.sort
			((o1, o2) -> o1.getIssueUnixTimeStamp().compareTo(o2.getIssueUnixTimeStamp()));
		
		// for correct sorting order
		assert( buffaloAreaWeatherEvents.get(0).getIssueUnixTimeStamp() <= 
				buffaloAreaWeatherEvents.get
				(buffaloAreaWeatherEvents.size()-1).getIssueUnixTimeStamp() );
		
		System.out.println("start unix time "+buffaloAreaWeatherEvents.get(0).getIssueUnixTimeStamp()
				+" end unix time "+buffaloAreaWeatherEvents.get
				(buffaloAreaWeatherEvents.size()-1).getIssueUnixTimeStamp()
				+ "avg active time "+(this.sumActiveTime/buffaloAreaWeatherEvents.size()));
	}*/
	
	private long getUnixTimeStamp(String timestamp)
	{
		long unixtime;
		try
		{
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
	
	
	private List<List<GlobalCoordinate>> convertToListOfPolygons(JSONArray polygonsArr, 
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
	
	
	private List<GlobalCoordinate> processAPolygonFromGeoJSON( JSONArray polygonsArr, 
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
	
	
//	private void updateBounds(double latitude, double longitude)
//	{
//		if( latitude <= minimumLat )
//		{
//			minimumLat = latitude;
//		}
//		
//		if( latitude >= maximumLat )
//		{
//			maximumLat = latitude;
//		}
//		
//		
//		if( longitude <= minimumLong )
//		{
//			minimumLong = longitude;
//		}
//		
//		if( longitude >= maximumLong )
//		{
//			maximumLong = longitude;
//		}
//	}
	
//	private boolean checkIfPolygonFallsInBuffalo( 
//			List<GlobalCoordinate> polygonList )
//	{
//		if( polygonList.size() <= 0 )
//			assert(false);
//		
//		Path2D geoJSONPolygon = new Path2D.Double();
//		GlobalCoordinate gCoord = polygonList.get(0);
//		geoJSONPolygon.moveTo( gCoord.getLatitude(), gCoord.getLongitude() );
//		for( int i = 1; i<polygonList.size(); ++i )
//		{
//			gCoord = polygonList.get(i);
//			geoJSONPolygon.lineTo( gCoord.getLatitude(), 
//					gCoord.getLongitude() );
//		}
//		geoJSONPolygon.closePath();
//		Rectangle2D boundingRect = geoJSONPolygon.getBounds2D();
//		
//		double minLat  = boundingRect.getMinX();
//		double maxLat  = boundingRect.getMaxX();
//		double minLong = boundingRect.getMinY();
//		double maxLong = boundingRect.getMaxY();
//		
//		if(checkRegionForBuffalo( minLat, maxLat, minLong, maxLong ) )
//		{
//			return true;
//		}
//		return false;
//	}
	
//	private boolean checkRegionForBuffalo( double minLat, double maxLat, 
//			double minLong, double maxLong )
//	{
////		public static final double minBuffaloLat 			= 42.0;
////		public static final double maxBuffaloLat 			= 44.0;
////		
////		public static final double minBuffaloLong			= -80.0;
////		public static final double maxBuffaloLong 			= -78.0;
//		
//		if( checkOverlap( SearchAndUpdateDriver.minBuffaloLat, SearchAndUpdateDriver.maxBuffaloLat, minLat, maxLat ) &&
//			checkOverlap( SearchAndUpdateDriver.minBuffaloLong, SearchAndUpdateDriver.maxBuffaloLong, minLong, maxLong ) )
//		{
//			return true;
//		}
//		return false;
//	}
	
	
	private boolean checkOverlap( double val1, double val2, double val3, double val4 )
	{
//		selectTableSQL = selectTableSQL +" ( "
//		+ "( "+lowerAttr+" <= "+queryMin +" AND "+upperAttr+" > "+queryMin+" ) OR "
//		+ "( "+lowerAttr+" <= "+queryMax +" AND "+upperAttr+" > "+queryMax+" ) OR "
//		+ "( "+lowerAttr+" >= "+queryMin +" AND "+upperAttr+" <= "+queryMax+" ) "+" ) ";
		
		if( ( (val1 <= val3) && (val2 >= val3) ) || 
			( (val1 <= val4) && (val2 >= val4) ) ||
			( (val1 >= val3) && (val2 <= val4) ) )
		{
			return true;
		}
		return false;
	}
	
	public static void main( String[] args )
	{
		WeatherDataProcessing weatherData 
								= new WeatherDataProcessing();
		weatherData.printAreaBounds();
		//weatherData.readTheWeatherFile();
		//weatherData.filterBuffaloWeatherEvents();
	}
}