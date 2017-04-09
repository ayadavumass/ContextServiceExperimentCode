package edu.umass.cs.largescalecasestudy;

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


public class WeatherBasedSearchQueryIssue extends 
						AbstractRequestSendingClass implements Runnable
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
	
	//private final DateFormat dfm 								= new SimpleDateFormat("yyyyMMddHHmm");
	
	
	private List<WeatherEventStorage> sortedWeatherEventList;
	private long sumSearchReply									= 0;
	private long numSearch 										= 0;
	private long sumSearchLatency 								= 0;
	
	
	public WeatherBasedSearchQueryIssue(double lossTolerance) 
	{
		super(lossTolerance);
		sortedWeatherEventList = new LinkedList<WeatherEventStorage>();
		readTheWeatherFile();
	}
	
	
	@Override
	public void run()
	{
		try
		{
			this.startExpTime();
			//
			if(LargeNumUsers.rateWorkload)
			{
				if(LargeNumUsers.backTobackReq)
				{
					backtoBackRequestSender();
				}
				else
				{
					rateBasedSender();
				}
			}
			else
				timestampBasedRequestSender();
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void timestampBasedRequestSender() throws Exception
	{
		int nextIndexNum = 0;
		WeatherEventStorage currEvent = null;
		while( LargeNumUsers.currRealUnixTime 
							< LargeNumUsers.END_UNIX_TIME )
		{
			if(nextIndexNum < sortedWeatherEventList.size())
			{
				currEvent = sortedWeatherEventList.get(nextIndexNum);
				
				while(currEvent.getIssueUnixTimeStamp() < LargeNumUsers.currRealUnixTime)
				{
					processAndIssueWeatherQuery(currEvent);
					nextIndexNum++;
					currEvent = sortedWeatherEventList.get(nextIndexNum);
				}
			}
			else
			{
				break;
			}
			Thread.sleep(LargeNumUsers.TIME_UPDATE_SLEEP_TIME);
		}
		
		System.out.println("WeatherSearch sending finished");
		
		waitForFinish();
		
		System.out.println("Weather search query sending ends "
				+ "avg search reply "+(sumSearchReply/numSearch)
				+ " sumSearchLatency "+(sumSearchLatency/numSearch)
				+ " numSearchRecvd "+numSearch+" numSent "+numSent);
	}
	
	
	public void backtoBackRequestSender() throws Exception
	{
		for(int k=0; k<sortedWeatherEventList.size(); k++)
		{
			WeatherEventStorage currEvent = sortedWeatherEventList.get(k);
			
			List<List<GlobalCoordinate>> polygonsList = currEvent.getListOfPolygons();
			
			for( int i=0; i<polygonsList.size(); i++ )
			{
				List<GlobalCoordinate> polygon =  polygonsList.get(i);
				
				Path2D geoJSONPolygon = new Path2D.Double();
				GlobalCoordinate gCoord = polygon.get(0);
				geoJSONPolygon.moveTo( gCoord.getLatitude(), gCoord.getLongitude() );
				for( int j = 1; j<polygon.size(); ++j )
				{
					gCoord = polygon.get(j);
					geoJSONPolygon.lineTo( gCoord.getLatitude(), 
							gCoord.getLongitude() );
				}
				geoJSONPolygon.closePath();
				Rectangle2D boundingRect = geoJSONPolygon.getBounds2D();
				
				double minLat  = boundingRect.getMinX();
				double maxLat  = boundingRect.getMaxX();
				double minLong = boundingRect.getMinY();
				double maxLong = boundingRect.getMaxY();
				
				if( (minLat >= LargeNumUsers.MIN_US_LAT) 
						&& (maxLat <= LargeNumUsers.MAX_US_LAT) 
						&& (minLong >= LargeNumUsers.MIN_US_LONG) 
						&& (maxLong <= LargeNumUsers.MAX_US_LONG) )
				{
					numSent++;
					
					String searchQuery = LargeNumUsers.LATITUDE_KEY+" >= "+minLat+
								" AND "+LargeNumUsers.LATITUDE_KEY+" <= "+maxLat
								+" AND "+LargeNumUsers.LONGITUDE_KEY+" >= "+
								minLong+" AND "+LargeNumUsers.LONGITUDE_KEY+" <= "+maxLong;
						
					//ExperimentSearchReply searchRep 
					//					= new ExperimentSearchReply( requestId );
					
					long start = System.currentTimeMillis();
					// not used without triggers.
					long queryExpiry = 900000;
					int repSize = LargeNumUsers.csClient.sendSearchQuery(searchQuery, 
												new JSONArray(), queryExpiry);
					
					long end = System.currentTimeMillis();
					sumSearchLatency = sumSearchLatency + (end-start);
					sumSearchReply = sumSearchReply + repSize;
					//LargeNumUsers.csClient.sendSearchQueryWithCallBack
					//			( searchQuery, queryExpiry, searchRep, this.getCallBack() );
					
					if(numSent%10 == 0)
					{
						System.out.println("Search stats avg latency "
								+(sumSearchLatency/numSent)+" avg rep size "+(sumSearchReply/numSent));
					}
				}
				else
				{
					System.out.println("Weather alert outside the area "+minLat
								+" , "+maxLat+" , "+minLong+" , "+maxLong);
				}
			}
		}
		
		System.out.println("Back to back Weather search query sending ends avg latency "
					+(sumSearchLatency/numSent)+" avg rep size "+(sumSearchReply/numSent));
	}
	
	
	private void rateBasedSender() throws Exception
	{
		List<List<GlobalCoordinate>> totalPolygonList 
							= new LinkedList<List<GlobalCoordinate>>();
		
		for(int k=0; k<sortedWeatherEventList.size(); k++)
		{
			WeatherEventStorage currEvent = sortedWeatherEventList.get(k);
			
			List<List<GlobalCoordinate>> polygonsList = currEvent.getListOfPolygons();
			
			for( int i=0; i<polygonsList.size(); i++ )
			{
				List<GlobalCoordinate> polygon =  polygonsList.get(i);
				totalPolygonList.add(polygon);
			}
		}
		
		
		int currPoly = 0;
		
		
		long currTime  = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = LargeNumUsers.requestsps;
		
		
		while( ( (System.currentTimeMillis() - expStartTime) 
						< 100000 ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				List<GlobalCoordinate> polygon = totalPolygonList.get(currPoly);
				currPoly++;
				currPoly = currPoly % totalPolygonList.size();
				
				doSearch(polygon);
			}
			currTime = System.currentTimeMillis();
			
			double timeElapsed = ((currTime- expStartTime)*1.0)/1000.0;
			double numberShouldBeSentByNow = timeElapsed*LargeNumUsers.requestsps;
			double needsToBeSentBeforeSleep = numberShouldBeSentByNow - numSent;
			if(needsToBeSentBeforeSleep > 0)
			{
				needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
			}
			
			for(int i=0;i<needsToBeSentBeforeSleep;i++)
			{
				List<GlobalCoordinate> polygon = totalPolygonList.get(currPoly);
				currPoly++;
				currPoly = currPoly % totalPolygonList.size();
				
				doSearch(polygon);
			}
			Thread.sleep(1000);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("WeatherSearch eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("WeatherSearch result:Goodput "+sysThrput);
	}
	
	private void doSearch(List<GlobalCoordinate> polygon) throws JSONException
	{	
		Path2D geoJSONPolygon = new Path2D.Double();
		GlobalCoordinate gCoord = polygon.get(0);
		geoJSONPolygon.moveTo( gCoord.getLatitude(), gCoord.getLongitude() );
		for( int j = 1; j<polygon.size(); ++j )
		{
			gCoord = polygon.get(j);
			geoJSONPolygon.lineTo( gCoord.getLatitude(), 
					gCoord.getLongitude() );
		}
		geoJSONPolygon.closePath();
		Rectangle2D boundingRect = geoJSONPolygon.getBounds2D();
		
		double minLat  = boundingRect.getMinX();
		double maxLat  = boundingRect.getMaxX();
		double minLong = boundingRect.getMinY();
		double maxLong = boundingRect.getMaxY();
		
		if( (minLat >= LargeNumUsers.MIN_US_LAT) 
				&& (maxLat <= LargeNumUsers.MAX_US_LAT) 
				&& (minLong >= LargeNumUsers.MIN_US_LONG) 
				&& (maxLong <= LargeNumUsers.MAX_US_LONG) )
		{
			long requestId = numSent++;
			
			
			if(!LargeNumUsers.localMySQLOper)
			{
				String searchQuery = LargeNumUsers.LATITUDE_KEY+" >= "+minLat+
						" AND "+LargeNumUsers.LATITUDE_KEY+" <= "+maxLat
						+" AND "+LargeNumUsers.LONGITUDE_KEY+" >= "+
						minLong+" AND "+LargeNumUsers.LONGITUDE_KEY+" <= "+maxLong;
				
				ExperimentSearchReply searchRep 
								= new ExperimentSearchReply( requestId );
				
				// not used without triggers.
				long queryExpiry = 900000;
				LargeNumUsers.csClient.sendSearchQueryWithCallBack
						( searchQuery, queryExpiry, searchRep, this.getCallBack() );
			}
			else
			{
				String mysqlQuery = "SELECT nodeGUID from attrIndexDataStorage WHERE "
						+ "(  ( latitude >= "+minLat+" AND latitude <= "+maxLat+" ) "
						+ "AND  ( longitude >= "+minLong+" AND longitude <= "+
							maxLong+" ) )";
				
				SearchTask stask = new SearchTask( mysqlQuery, this );
				LargeNumUsers.taskES.execute(stask);
			}
		}
		else
		{
			System.out.println("Weather alert outside the area "+minLat
						+" , "+maxLat+" , "+minLong+" , "+maxLong);
		}
	}
	
	
	
	private void processAndIssueWeatherQuery(WeatherEventStorage weatherEvent)
	{
		List<List<GlobalCoordinate>> polygonsList = weatherEvent.getListOfPolygons();
		
		for( int i=0; i<polygonsList.size(); i++ )
		{
			List<GlobalCoordinate> polygon =  polygonsList.get(i);
			
			Path2D geoJSONPolygon = new Path2D.Double();
			GlobalCoordinate gCoord = polygon.get(0);
			geoJSONPolygon.moveTo( gCoord.getLatitude(), gCoord.getLongitude() );
			for( int j = 1; j<polygon.size(); ++j )
			{
				gCoord = polygon.get(j);
				geoJSONPolygon.lineTo( gCoord.getLatitude(), 
						gCoord.getLongitude() );
			}
			geoJSONPolygon.closePath();
			Rectangle2D boundingRect = geoJSONPolygon.getBounds2D();
			
			double minLat  = boundingRect.getMinX();
			double maxLat  = boundingRect.getMaxX();
			double minLong = boundingRect.getMinY();
			double maxLong = boundingRect.getMaxY();
			
			if( (minLat >= LargeNumUsers.MIN_US_LAT) 
					&& (maxLat <= LargeNumUsers.MAX_US_LAT) 
					&& (minLong >= LargeNumUsers.MIN_US_LONG) 
					&& (maxLong <= LargeNumUsers.MAX_US_LONG) )
			{
				long requestId = numSent++;
				
				
				if(!LargeNumUsers.localMySQLOper)
				{
					String searchQuery = LargeNumUsers.LATITUDE_KEY+" >= "+minLat+
							" AND "+LargeNumUsers.LATITUDE_KEY+" <= "+maxLat
							+" AND "+LargeNumUsers.LONGITUDE_KEY+" >= "+
							minLong+" AND "+LargeNumUsers.LONGITUDE_KEY+" <= "+maxLong;
					
					ExperimentSearchReply searchRep 
									= new ExperimentSearchReply( requestId );
					
					// not used without triggers.
					long queryExpiry = 900000;
					LargeNumUsers.csClient.sendSearchQueryWithCallBack
							( searchQuery, queryExpiry, searchRep, this.getCallBack() );
					
					// to prevent busting or testing its effect
					try 
					{
						Thread.sleep(100);
					} catch (InterruptedException e) 
					{
						e.printStackTrace();
					}
					
				}
				else
				{
					String mysqlQuery = "SELECT nodeGUID from attrIndexDataStorage WHERE "
							+ "(  ( latitude >= "+minLat+" AND latitude <= "+maxLat+" ) "
							+ "AND  ( longitude >= "+minLong+" AND longitude <= "+
								maxLong+" ) )";
					
					SearchTask stask = new SearchTask( mysqlQuery, this );
					LargeNumUsers.taskES.execute(stask);
				}
			}
			else
			{
				System.out.println("Weather alert outside the area "+minLat
							+" , "+maxLat+" , "+minLong+" , "+maxLong);
			}
		}
	}
	
	
	private long getUnixTimeStamp(String timestamp)
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
	
	public void readTheWeatherFile()
	{
		boolean featuresFound = false;
		BufferedReader br = null;
		
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader
					(new FileReader(LargeNumUsers.WEATHER_DATA_PATH));
			
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
						
						long issueTS = getUnixTimeStamp(issueTime);
						
						if( (issueTS >= LargeNumUsers.START_UNIX_TIME) 
										&& (issueTS <= LargeNumUsers.END_UNIX_TIME) )
						{
							WeatherEventStorage weatherEvents = new WeatherEventStorage
									( WFO, issueTime, expireTime, phenomCode, 
										getUnixTimeStamp(issueTime), getUnixTimeStamp(expireTime), 
										polygons, Double.parseDouble(areaKm2) );
							
							assert(weatherEvents != null);
							sortedWeatherEventList.add(weatherEvents);
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
		sortedWeatherEventList.sort(new WeatherEventStorage( "", "", "", 
				"", -1, -1, null, -1));
		
		System.out.println("Number of weather events in the timeslot "
												+sortedWeatherEventList.size());
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

	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{	
	}

	@Override
	public void incrementSearchNumRecvd(int resultSize, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			numSearch++;
			sumSearchReply = sumSearchReply + resultSize;
			
			sumSearchLatency = sumSearchLatency + timeTaken;
			if(numSearch % 10 == 0)
				System.out.println(" Search rep recvd avg search reply "+(sumSearchReply/numSearch)
					+ " sumSearchLatency "+(sumSearchLatency/numSearch)+" numSearchRecvd "+numSearch
					+ " numSearchSent "+numSent);
			
			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				waitLock.notify();
			}
		}
	}
	
	public static void main(String[] args)
	{
		//readTheWeatherFile();
	}
}