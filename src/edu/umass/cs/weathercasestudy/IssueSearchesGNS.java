package edu.umass.cs.weathercasestudy;

import java.awt.geom.Path2D;
import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import edu.umass.cs.acs.geodesy.GlobalCoordinate;


public class IssueSearchesGNS extends AbstractRequestSendingClass
{
//	public static final String LAT_ATTR_NAME					= "latitude";
//	public static final String LONG_ATTR_NAME					= "longitude";
//	public static final double MIN_UNIX_TIME					= 1385770103;
//	public static final double MAX_UNIX_TIME					= 1391127928;
	
	// in ms
//	public static final double TIME_CONTRACTION_EXP_TIME		= 1000.0; // unit is ms
//	// for 1 sec
//	public static final double TIME_CONTRACTION_REAL_TIME 		= 17859.416666667;  // unit is s
	
	public static final double PERIODIC_REFRESH_SLEEP_TIME		= 10;  // 10 ms. 1000 ms exp time = 300 mins real time
	                                                               // so setting it to 10 ms low value
	
	public static final double TIME_UPDATE_SLEEP_TIME			= 10;  // 10 ms. 1000 ms exp time = 300 mins real time
    // so setting it to 10 ms low value
	
	
	private WeatherDataProcessing weatherDataProcess;
	
//	private static String csHost;
//	private static int csPort;
	
//	public static ContextServiceClient<String> csClient;
//	public static boolean useGNS								= false;
	
	private double simulatedTime;
	private int nextIndexToSend 								= 0;
	
	private long requestId										= 0;
	
	private double sumSearchLatency								= 0;
	private double sumResultSize								= 0;
	
	//private double currentRealTime							= SearchAndUpdateDriver.MIN_UNIX_TIME;
	
	//private final ConcurrentHashMap<String, ActiveQueryStorage> activeQMap;
	//private final TimerThread timerThread;
	//private final double refreshTimeInSec						= 300;  // 300 s, 5 min refresh time
	
	//public static int threadPoolSize							= 1;
	
	//public static List<GuidEntry> listOfGuidEntries			= null;
	//public static final Object guidInsertLock					= new Object();
	
	//public static Queue<GNSClient> gnsClientQueue				= new LinkedList<GNSClient>();
	//public static final Object queueLock						= new Object();
	
	
	public IssueSearchesGNS()
				throws NoSuchAlgorithmException, IOException
	{
		super(SearchAndUpdateDriver.SEARCH_LOSS_TOLERANCE);
		
		weatherDataProcess 		 = new WeatherDataProcessing();
		//activeQMap 				 = new ConcurrentHashMap<String, ActiveQueryStorage>();
		//timerThread				 = new TimerThread();
		
//		if( csHost != null )
//			csClient  = new ContextServiceClient<String>(csHost, csPort, 
//						ContextServiceClient.HYPERSPACE_BASED_CS_TRANSFORM);
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
//			System.out.println("Updates recvd "+userGUID+" time "+timeTaken
//					+" numRecvd "+numRecvd+" numSent "+numSent);
			this.sumSearchLatency = this.sumSearchLatency + timeTaken;
			this.sumResultSize = this.sumResultSize + resultSize;
			if(checkForCompletionWithLossTolerance(numSent, numRecvd))
			{
				waitLock.notify();
			}
		}
	}
	
	public void runSearches() throws InterruptedException
	{	
		long start = System.currentTimeMillis();
		simulatedTime = SearchAndUpdateDriver.MIN_UNIX_TIME;
		while( simulatedTime <= SearchAndUpdateDriver.MAX_UNIX_TIME )
		{
			Date date = new Date((long)simulatedTime*1000L); 
						// *1000 is to convert seconds to milliseconds
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); 
						// the format of your date
			sdf.setTimeZone(TimeZone.getTimeZone("GMT")); 
						// give a timezone reference for formating (see comment at the bottom
			String dateFormat = sdf.format(date);
			System.out.println("Search: Current simulated time "+simulatedTime+" time in GMT-5 "
					+dateFormat+" numSent "+numSent+" numRecvd "+numRecvd+" avg reply size "+(sumResultSize/numRecvd));
			sendSearchesWhoseTimeHasCome(simulatedTime);
			Thread.sleep(1000);
			simulatedTime = simulatedTime +SearchAndUpdateDriver.TIME_CONTRACTION_REAL_TIME;
			
		}
		long end = System.currentTimeMillis();
		double sendingRate = (numSent*1000.0)/(end-start);
		System.out.println("Search eventual sending rate "+sendingRate+" reqs/s");
		this.waitForFinish();
		long endTime = System.currentTimeMillis();
		double systemThpt = (numRecvd*1000.0)/(endTime-start);
		System.out.println("Search system throughput "+systemThpt+" reqs/s");
		System.out.println("Search avg search latency "+(sumSearchLatency/numRecvd)+" ms "
				+" result size "+(sumResultSize/numRecvd));
	}
	
	private void sendSearchesWhoseTimeHasCome(double simulatedTime)
	{
		List<WeatherEventStorage> buffaloWeatherList 
							= weatherDataProcess.getBuffaloAreaWeatherEvents();
		
		while( nextIndexToSend < buffaloWeatherList.size() )
		{
			WeatherEventStorage currWeatherEvent 
										= buffaloWeatherList.get(nextIndexToSend);
			if( currWeatherEvent.getIssueUnixTimeStamp() <=  simulatedTime )
			{
				sendSearchQuery(currWeatherEvent, requestId++);
				nextIndexToSend++;
			}
			else
			{
				break;
			}
		}
	}
	
	private void sendSearchQuery(WeatherEventStorage currWeatherEvent, long requestId)
	{
		List<List<GlobalCoordinate>> polygonsList = currWeatherEvent.getListOfPolygons();
		
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
			
//			String searchQuery
//				= "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE "+LAT_ATTR_NAME+" >= "+minLat+
//				" AND "+LAT_ATTR_NAME+" <= "+maxLat+" AND "+LONG_ATTR_NAME+" >= "+
//				minLong+" AND "+LONG_ATTR_NAME+" <= "+maxLong;
			
			String searchQuery = generateAGNSSelectQuery( minLat, maxLat, 
					minLong, maxLong );
			
			SearchUsingStringTask sTask = new SearchUsingStringTask(searchQuery, this);
			numSent++;
			SearchAndUpdateDriver.taskES.execute(sTask);
		}
	}
	
	
	private String generateAGNSSelectQuery( double minLat, double maxLat, 
			double minLong, double maxLong )
	{
		String gnsSearchQ = "$and:[";
		
		String predicate1 = getAPredicateMongoString(SearchAndUpdateDriver.latitudeAttr , minLat, 
				maxLat );
		
		gnsSearchQ = gnsSearchQ + predicate1+" , ";
		
		String predicate2 = getAPredicateMongoString(SearchAndUpdateDriver.longitudeAttr , minLong, 
				maxLong );
		
		gnsSearchQ = gnsSearchQ + predicate2+" ] ";
		
		return gnsSearchQ;
	}
	
	private String getAPredicateMongoString(String attrName, double attrMin, double attrMax)
	{
		// normal case
		if(attrMin <= attrMax)
		{
			String query = "("+"\"~"+attrName+"\":($gt:"+attrMin+",$lt:"+attrMax+")"+")";
			return query;
		}
		else // circular query case
		{
//			//$or:[("~hometown":"whoville"),("~money":($gt:0))]
//			String pred1 = "("+"\"~"+attrName+"\":($gt:"+attrMin+",$lt:"
//													+ SearchAndUpdateDriver.AT+")"+")";
//			String pred2 = "("+"\"~"+attrName+"\":($gt:"+SearchAndUpdateDriver.ATTR_MIN
//													+ ",$lt:"+attrMax+")"+")";
//			
//			String query = "("+"$or:["+pred1+","+pred2+"]" +")";
//			return query;
			assert(false);
			return null;
		}
	}
	
	
	public static void main( String[] args ) throws NoSuchAlgorithmException, IOException, InterruptedException
	{
//		csHost = args[0];
//		csPort = Integer.parseInt(args[1]);
		IssueSearchesGNS issueSearch = new IssueSearchesGNS();
		issueSearch.runSearches();
	}
}