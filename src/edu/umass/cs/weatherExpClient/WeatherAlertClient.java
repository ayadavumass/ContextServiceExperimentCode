package edu.umass.cs.weatherExpClient;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.acs.geodesy.GlobalCoordinate;
import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;

public class WeatherAlertClient
{
	public static final String TRACE_START_TIME = "2015-12-26T15:00:00";
	public static final String TRACE_END_TIME   = "2015-12-26T23:00:13";
	
	private static final SimpleDateFormat dateFormat 
											= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private static final double LEFT 		= -98.08;
	private static final double RIGHT 		= -96.01;
	private static final double TOP 		= 33.635;
	private static final double BOTTOM 		= 31.854;
	
	
	private static final GlobalCoordinate UPPER_LEFT = new GlobalCoordinate(TOP, LEFT);
	//private static final GlobalCoordinate UPPER_LEFT = new GlobalCoordinate(33.45, -98.08);
	private static final GlobalCoordinate UPPER_RIGHT = new GlobalCoordinate(TOP, RIGHT);
	//private static final GlobalCoordinate UPPER_RIGHT = new GlobalCoordinate(33.45, -96.01);
	private static final GlobalCoordinate LOWER_RIGHT = new GlobalCoordinate(BOTTOM, RIGHT);
	//private static final GlobalCoordinate LOWER_RIGHT = new GlobalCoordinate(32.23, -96.01);
	private static final GlobalCoordinate LOWER_LEFT = new GlobalCoordinate(BOTTOM, LEFT);
	//private static final GlobalCoordinate LOWER_LEFT = new GlobalCoordinate(32.23, -98.08);
	
	public static final List<GlobalCoordinate> AREA_EXTENT = new ArrayList(
	          Arrays.asList(UPPER_LEFT, UPPER_RIGHT, LOWER_RIGHT, LOWER_LEFT, UPPER_LEFT));
	
	public static final String weatherDataDir 			 
	= "/proj/MobilityFirst/ayadavDir/ACSGithub/Alert-Control-System/data/ALL-12-26";
	
	public static final String GEO_LOCATION_CURRENT_LAT  = "geoLocationCurrentLat"; // Users current location (dynamic)
	public static final String GEO_LOCATION_CURRENT_LONG = "geoLocationCurrentLong"; // Users current location (dynamic)

	public static final String GEO_LOCATION_WORK_LAT     = "geoLocationWorkLat"; // Work custom location
	public static final String GEO_LOCATION_WORK_LONG    = "geoLocationWorkLong"; // Work custom location
	
	public static final String GEO_LOCATION_HOME_LAT     = "geoLocationHomeLat"; // Home custom location
	public static final String GEO_LOCATION_HOME_LONG    = "geoLocationHomeLong"; // Home custom location
	
	
	public static final String featureConstant			 = "Feature";
	public static final String polygonConstant			 = "Polygon";
	public static final String geometryField 			 = "geometry";
	public static final String typeField     			 = "type";
	public static final String coordinatesField     	 = "coordinates";
	
	
	private final ContextServiceClient<String> csClient;
	private final GNSClientCommands gnsClient;
	
	private final Timer clockTimer;
	
	private long currUnixTimeStamp;
	
	// if false it will use gns
	private static boolean useContextService	= false;
	
	public WeatherAlertClient(String gnsHost, int gnsPort, String csHost, int csPort) 
											throws IOException, ParseException, NoSuchAlgorithmException
	{
		csClient = new ContextServiceClient<String>(csHost, csPort, 
				ContextServiceClient.SUBSPACE_BASED_CS_TRANSFORM);
		clockTimer = new Timer();
		 
		boolean disableSSL = true;
		System.out.println("gnsHost "+gnsHost+" gnsPort "+gnsPort);
		gnsClient = new GNSClientCommands();
		//useContextService = true;
		currUnixTimeStamp = dateFormat.parse(TRACE_START_TIME).getTime();
		clockTimer.schedule(new ClockTimer(),
	               60*1000,        //initial delay
	               60*1000);  //subsequent rate
	}
	
	private class ClockTimer extends TimerTask 
	{	
	    public void run() 
	    {
	    	// adding 1 minute
	    	currUnixTimeStamp = currUnixTimeStamp + (60*1000);
	    }
	}
	
	/**
	 * sample properties
	 * "properties": {
	 * "ReflectivityLevel": 30,
	 * "event": "Severe weather",
	 * "expires": "2015-12-26T23:02:39-06",
	 * "headline": "Rain forecasted",
	 * "simpleGeom": true,
	 * "summary": "Rain is predicted in 5 minutes by CASA",
	 * "timestamp": "2015-12-26T22:52:39-06",
	 * "validAt": "2015-12-26T22:57:39-06"
	 * }
    */
	private void runWeatherTrace()
	{
		try ( BufferedReader br = new BufferedReader(new FileReader("weather3HourTrace.txt")) )
		{
			String sCurrentLine;
			
			while ( (sCurrentLine = br.readLine()) != null )
			{
				JSONObject weatherJSON = readFileAndConjureAJSON(sCurrentLine);
				
				try
				{
					List<GlobalCoordinate> polygonCoordList = new LinkedList<GlobalCoordinate>();
					
					// geoJSON type
					String weatherPolygonType = weatherJSON.getString(typeField);
					// currently it is only done for feature, which have one polygon
					// not for featureCollection
					try
					{
						assert(weatherPolygonType.equals(featureConstant));
					} catch(Error ex)
					{
						System.out.println("not feature for "+sCurrentLine);
						//ex.printStackTrace();
						continue;
					}
					
					JSONObject geometryJSONObject = weatherJSON.getJSONObject(geometryField);
					// the way it is set in files for now
					JSONArray coordinateArray = geometryJSONObject.getJSONArray(coordinatesField).getJSONArray(0);
					
					String typeOfPolygon      = geometryJSONObject.getString(typeField);
					
					try
					{
					assert(typeOfPolygon.equals(polygonConstant));
					}
					catch(Error ex)
					{
						System.out.println("not polygon for "+sCurrentLine);
						continue;
					}
					
					for(int i=0; i<coordinateArray.length();i++)
					{
						// it is in form of longitude, latitude
						JSONArray coordArray = coordinateArray.getJSONArray(i);
						//System.out.println("JSONArray coordArray "+coordArray);
						
						double latitude  = coordArray.getDouble(1);
						double longitude = coordArray.getDouble(0);
						
						GlobalCoordinate gCoord = new GlobalCoordinate(latitude, longitude);
						polygonCoordList.add(gCoord);
					}
					
					if(useContextService)
					{
						String contextQuery = GnsDatabase.buildContextServiceQuery(GEO_LOCATION_CURRENT_LAT, GEO_LOCATION_CURRENT_LONG, 
								polygonCoordList);
						JSONArray resultArray = new JSONArray();
						long expiryTime = 300000;
						
						long start = System.currentTimeMillis();
						csClient.sendSearchQuery(contextQuery, resultArray, expiryTime);
						long end = System.currentTimeMillis();
						System.out.println("Filename "+ sCurrentLine +" time taken by CS "+(end-start) +" result size "
								+resultArray.length());
					}
					else
					{
						String gnsSelectQuery = GnsDatabase.buildLocationsQuery(polygonCoordList);
						long start = System.currentTimeMillis();
						JSONArray resultGUID = gnsClient.selectQuery(gnsSelectQuery);
						long end = System.currentTimeMillis();
						
						System.out.println("Filename "+ sCurrentLine +" time taken by GNS "+(end-start)+" result size "
								+resultGUID.length());
					}
				} catch (JSONException e)
				{
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * tests area extent for all guids before running
	 * the trace.
	 */
	private void testAreaExtent(int totalNumGUIDsExpected)
	{
		try
		{
			if(useContextService)
			{
				String contextQuery = GnsDatabase.buildContextServiceQuery(GEO_LOCATION_CURRENT_LAT, GEO_LOCATION_CURRENT_LONG, 
						AREA_EXTENT);
				
				//ConcurrentHashMap<String, Boolean> resultGUIDMap = new ConcurrentHashMap<String, Boolean>();
				JSONArray resultArray = new JSONArray();
				long expiryTime = 300000;
				
				long start = System.currentTimeMillis();
				csClient.sendSearchQuery(contextQuery, resultArray, 
						expiryTime);
				long end = System.currentTimeMillis();
				System.out.println("AREA_EXTENT "+" time taken by CS "+(end-start) +" result size "
						+resultArray.length());
				assert(totalNumGUIDsExpected <= resultArray.length());
			}
			else
			{
				String gnsSelectQuery = GnsDatabase.buildLocationsQuery(AREA_EXTENT);
				long start = System.currentTimeMillis();
				System.out.println("Select started");
				JSONArray resultGUID = gnsClient.selectQuery(gnsSelectQuery);
				long end = System.currentTimeMillis();
				
				System.out.println("AREA_EXTENT "+" time taken by GNS "+(end-start)+" result size "
						+resultGUID.length());
				assert(totalNumGUIDsExpected <= resultGUID.length());
			}
		} catch(Exception | Error ex)
		{
			ex.printStackTrace();
		}
	}
	
	private static JSONObject readFileAndConjureAJSON(String fileName)
	{
		BufferedReader br = null;
		String jsonString = "";
		try
		{
			String sCurrentLine;
			br = new BufferedReader(new FileReader(weatherDataDir+"/"+fileName));
			
			while ((sCurrentLine = br.readLine()) != null) 
			{
				jsonString = jsonString + sCurrentLine;
			}
		} catch (IOException e) 
		{
			e.printStackTrace();
		} finally 
		{
			try
			{
				if (br != null)br.close();
			} catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
		
		try 
		{
			JSONObject retJSON = new JSONObject(jsonString);
			return retJSON;
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return null;
	}
	
	public static void main(String[] args) throws NoSuchAlgorithmException
	{
		int numUsers = Integer.parseInt(args[0]);
		String gnsHost  = args[1];
		int gnsPort = Integer.parseInt(args[2]);
		String csHost = args[3];
		int csPort = Integer.parseInt(args[4]);
		useContextService = Boolean.parseBoolean(args[5]);
		
		try
		{
			WeatherAlertClient wObj 
						= new WeatherAlertClient(gnsHost, gnsPort, csHost, csPort);
			System.out.println("WeatherAlertClient complete");
			wObj.testAreaExtent(numUsers);
			Thread.sleep(10000);
			wObj.runWeatherTrace();
		} catch (NumberFormatException e) 
		{
			e.printStackTrace();
		} catch (IOException e) 
		{
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}