package edu.umass.cs.weathercasestudy;

import java.awt.geom.Path2D;
import java.awt.geom.Rectangle2D;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.acs.geodesy.GlobalCoordinate;

public class WeatherDataProcessing
{
	// json key constants
	public static final String propertiesKey			= "properties";
	public static final String ISSUED_Key				= "ISSUED";
	public static final String EXPIREDKey				= "EXPIRED";
	public static final String PHENOMKey				= "PHENOM";
	public static final String geometryKey				= "geometry";
	public static final String typeKey					= "type";
	public static final String polygonKey				= "coordinates";
	
	
	// json key-value constants
	public static final String polygonVal				= "Polygon";
	public static final String multiPolygonVal			= "MultiPolygon";
	
	
	public static final double minBuffaloLat 			= 42.0;
	public static final double maxBuffaloLat 			= 44.0;
	
	public static final double minBuffaloLong			= -80.0;
	public static final double maxBuffaloLong 			= -78.0;
	
	///home/adipc/Downloads/weatherdata/wwa_201311300000_201401310000
	public static final String weatherDataPath 			
			= "/home/adipc/Downloads/weatherdata/wwa_201311300000_201401310000/weatherEvent.json";
	
	//public static final String weatherDataPath 			= "/users/ayadavum/weatherData/weatherEvent.json";
	
	private final List<JSONObject> weatherEventList;
	
	private final List<WeatherEventStorage> buffaloAreaWeatherEvents;
	
	private DateFormat dfm = new SimpleDateFormat("yyyyMMddHHmm");
	
	private double sumActiveTime						= 0.0;
	
	public WeatherDataProcessing()
	{
		weatherEventList = new LinkedList<JSONObject>();
		buffaloAreaWeatherEvents = new LinkedList<WeatherEventStorage>();
		readTheWeatherFile();
		filterBuffaloWeatherEvents();
		
		dfm.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
	public List<WeatherEventStorage> getBuffaloAreaWeatherEvents()
	{
		return 	buffaloAreaWeatherEvents;
	}
	
	public void readTheWeatherFile()
	{
		BufferedReader br = null;
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader(new FileReader(weatherDataPath));
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				if( sCurrentLine.startsWith("{") )
				{
					// exclude the main geoJSON
					if(sCurrentLine.length() > 2)
					{
						int commaIndex = sCurrentLine.length()-1;
						if( sCurrentLine.charAt(commaIndex) == ',' )
						{
							String removeLastComma = sCurrentLine.substring(0, commaIndex);
							JSONObject weatherEventJSON = new JSONObject(removeLastComma);
							weatherEventList.add(weatherEventJSON);
						}
						else
						{
							JSONObject weatherEventJSON = new JSONObject(sCurrentLine);
							weatherEventList.add(weatherEventJSON);
						}
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
		
		System.out.println("Number of weather events "+weatherEventList.size());
//		bw.close();
//		System.out.println( "unique users "
//							+userMobilityEntryHashMap.size() );
//		
//		Iterator<Integer> userIdIter = userMobilityEntryHashMap.keySet().iterator();
//		
//		while( userIdIter.hasNext() )
//		{
//			int userId = userIdIter.next();
//			
//			List<TrajectoryEntry> logEntryList 
//							= userMobilityEntryHashMap.get(userId);
//			
//			logEntryList.sort
//			((o1, o2) -> o1.getUnixTimeStamp().compareTo(o2.getUnixTimeStamp()));
//		}
	}
	
	private void filterBuffaloWeatherEvents()
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
				
				List<List<GlobalCoordinate>> buffaloWeatherEvents 
										= convertToListOfPolygons(polygonsArr, polygonType);
				
				if( buffaloWeatherEvents.size() > 0 )
				{
					
					long issuedUnixTime = getUnixTimeStamp(issuedTime);
					long finalUnixTime  = getUnixTimeStamp(expiredTime);
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
	}
	
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
		
		if( polygonType.equals(polygonVal) )
		{
			List<GlobalCoordinate> polygon = processAPolygonFromGeoJSON( polygonsArr,  polygonType);
			//assert(polygon != null);
			if(polygon != null)
				polygonsList.add(polygon);
		}
		else if( polygonType.equals(multiPolygonVal) )
		{
			for( int i=0; i<polygonsArr.length(); i++ )
			{
				try
				{
					JSONArray polygonsJSON = polygonsArr.getJSONArray(i);
					List<GlobalCoordinate> polygon = processAPolygonFromGeoJSON( polygonsJSON, polygonType );
					//assert(polygon != null);
					if(polygon != null)
						polygonsList.add(polygon);
				}
				catch (JSONException e)
				{
					e.printStackTrace();
				}
			}
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
					//System.out.println("coord "+coord);
					double longitude = coord.getDouble(0);
					double latitude = coord.getDouble(1);
					GlobalCoordinate gCoord = new GlobalCoordinate(latitude, longitude);
					polygonList.add(gCoord);
				}
				
				if(checkIfPolygonFallsInBuffalo( polygonList ))
				{
					return polygonList;
					//polygonsList.add(polygonList);
				}
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
	
	private boolean checkIfPolygonFallsInBuffalo( 
			List<GlobalCoordinate> polygonList )
	{
		if( polygonList.size() <= 0 )
			assert(false);
		
		Path2D geoJSONPolygon = new Path2D.Double();
		GlobalCoordinate gCoord = polygonList.get(0);
		geoJSONPolygon.moveTo( gCoord.getLatitude(), gCoord.getLongitude() );
		for( int i = 1; i<polygonList.size(); ++i )
		{
			gCoord = polygonList.get(i);
			geoJSONPolygon.lineTo( gCoord.getLatitude(), 
					gCoord.getLongitude() );
		}
		geoJSONPolygon.closePath();
		Rectangle2D boundingRect = geoJSONPolygon.getBounds2D();
		
		double minLat  = boundingRect.getMinX();
		double maxLat  = boundingRect.getMaxX();
		double minLong = boundingRect.getMinY();
		double maxLong = boundingRect.getMaxY();
		
		if(checkRegionForBuffalo( minLat, maxLat, minLong, maxLong ) )
		{
			return true;
		}
		return false;
	}
	
	private boolean checkRegionForBuffalo( double minLat, double maxLat, 
			double minLong, double maxLong )
	{
//		public static final double minBuffaloLat 			= 42.0;
//		public static final double maxBuffaloLat 			= 44.0;
//		
//		public static final double minBuffaloLong			= -80.0;
//		public static final double maxBuffaloLong 			= -78.0;
		
		if( checkOverlap( minBuffaloLat, maxBuffaloLat, minLat, maxLat ) &&
			checkOverlap( minBuffaloLong, maxBuffaloLong, minLong, maxLong ) )
		{
			return true;
		}
		return false;
	}
	
	
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
		//weatherData.readTheWeatherFile();
		//weatherData.filterBuffaloWeatherEvents();
	}
}