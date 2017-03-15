package edu.umass.cs.nwsdataprocessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
import edu.umass.cs.utils.UtilFunctions;

public class ProcessForSpecficTimeSlots 
{
	// json key constants
	public static final String PROPERTRY_KEY					= "properties";
	//public static final String ISSUED_KEY						= "ISSUED";
	public static final String ISSUED_KEY						= "INIT_ISS";
	public static final String EXPIRED_KEY						= "EXPIRED";
	public static final String PHENOM_KEY						= "PHENOM";
	public static final String GEOMETRY_KEY						= "geometry";
	public static final String TYPE_KEY							= "type";
	public static final String COORDINATE_KEY					= "coordinates";
	public static final String RADAR_INFO_KEY					= "WFO";  // stands for weather forecasting office.
	public static final String AREA_KM2_KEY						= "AREA_KM2";
		
	// json key-value constants
	public static final String POLYGON_TYPE						= "Polygon";
	public static final String MULTIPOLYGON_TYPE				= "MultiPolygon";
		
	// for 14th March 2017, 09:15 AM
	public static final long START_TIME							= 1489497300;
	
	// for 14th March 2017, 09:30 AM
	public static final long END_TIME							= 1489498200;
	
	
	public static final String GEOJSON_WEATHER_DATA_DIR			= "/home/ayadav/Documents/Data/weatherDataSnapShot/geojsondata";
	
	// stores distinct weather events in the timeslot
	public static HashMap<String, WeatherEventStorage> weatherEventMap;
	
	private static DateFormat dfm = new SimpleDateFormat("yyyyMMddHHmm");
	
	private static long getUnixTimeStamp(String timestamp)
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
	
	private static void processWeatherFile(String weatherFilePath)
	{
		boolean featuresFound = false;
		BufferedReader br = null;
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader
					(new FileReader(weatherFilePath));
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				if(featuresFound)
				{
					if( sCurrentLine.startsWith("{") )
					{
						String hash = UtilFunctions.getGUIDHash(sCurrentLine);
						// removing idential events across files
						if(weatherEventMap.containsKey(hash))
							continue;
						
						JSONObject weatherEventJSON = new JSONObject(sCurrentLine);
						JSONObject propertiesJSON = weatherEventJSON.getJSONObject(PROPERTRY_KEY);
						JSONObject geometryJSON = weatherEventJSON.getJSONObject(GEOMETRY_KEY);
						
						String WFO = propertiesJSON.getString(RADAR_INFO_KEY);
						String issueTime = propertiesJSON.getString(ISSUED_KEY);
						String expireTime = propertiesJSON.getString(EXPIRED_KEY);
						
						String phenomCode 
							= (propertiesJSON.has(PHENOM_KEY))?propertiesJSON.getString(PHENOM_KEY):"";
						String areaKm2 
							= (propertiesJSON.has(AREA_KM2_KEY))?propertiesJSON.getString(AREA_KM2_KEY):"";
						
						String geometryType = geometryJSON.getString(TYPE_KEY);
						JSONArray geometryArray = geometryJSON.getJSONArray(COORDINATE_KEY);
						
						List<List<GlobalCoordinate>> polygons 
								= convertToListOfPolygons(geometryArray, geometryType);
						
						WeatherEventStorage weatherEvents = new WeatherEventStorage
									( WFO, issueTime, expireTime, phenomCode, 
										getUnixTimeStamp(issueTime), getUnixTimeStamp(expireTime), 
										polygons, (areaKm2.length()>0)?Double.parseDouble(areaKm2):0 );
						
						weatherEventMap.put(hash, weatherEvents);
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
	
	public static void main(String[] args)
	{
		weatherEventMap = new HashMap<String, WeatherEventStorage>();
		
		File folder = new File(GEOJSON_WEATHER_DATA_DIR);
		File[] listOfFiles = folder.listFiles();
		
		for (int i = 0; i < listOfFiles.length; i++)
		{
			if( listOfFiles[i].isFile() )
			{
				//System.out.println("File " + listOfFiles[i].getName());
				
				String filename = listOfFiles[i].getName();
				
				if( filename.startsWith("WeatherAt") )
				{
					long timestamp = Long.parseLong(filename.split(".json")[0].split("-")[1]);
					if( (timestamp >= START_TIME) && (timestamp <= END_TIME) )
					{
						System.out.println("File in timeslot "+filename);
						processWeatherFile(GEOJSON_WEATHER_DATA_DIR
												+"/"+filename);
						
					}
				}
			}
			else if (listOfFiles[i].isDirectory())
			{
				//System.out.println("Directory " + listOfFiles[i].getName());
		    }
		}
		
		// printing total event and total polygon
		Iterator<String> hashIter = weatherEventMap.keySet().iterator();
		long totalPolygons = 0;
		while(hashIter.hasNext())
		{
			WeatherEventStorage event = weatherEventMap.get(hashIter.next());
			totalPolygons = totalPolygons + event.getListOfPolygons().size();
		}
		System.out.println("Total weather events "+weatherEventMap.size()
								+" polygons "+totalPolygons);	
	}
}