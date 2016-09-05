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
import edu.umass.cs.contextservice.client.ContextServiceClient;

public class IssueSearches extends AbstractRequestSendingClass
{
	public static final String LAT_ATTR_NAME				= "latitude";
	public static final String LONG_ATTR_NAME				= "longitude";
	
	
	public static final double SEARCH_LOSS_TOLERANCE       	= 0.0;
	public static final double MIN_UNIX_TIME				= 1385770103;
	public static final double MAX_UNIX_TIME				= 1391127928;
	
	public static final double timeContractionFactor 		= 17859.416666667;
	
	private WeatherDataProcessing weatherDataProcess;
	
	private static String csHost;
	private static int csPort;
	
	public static ContextServiceClient<String> csClient;
	public static boolean useGNS							= false;
	
	
	private double simulatedTime;
	private int nextIndexToSend 							= 0;
	
	private long requestId									= 0;
	
	private double sumSearchLatency							= 0;
	
	
	public IssueSearches() throws NoSuchAlgorithmException, IOException
	{
		super(SEARCH_LOSS_TOLERANCE);
		weatherDataProcess 		 = new WeatherDataProcessing();
		
		if( csHost != null )
			csClient  = new ContextServiceClient<String>(csHost, csPort, 
						ContextServiceClient.HYPERSPACE_BASED_CS_TRANSFORM);
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
			if(checkForCompletionWithLossTolerance(numSent, numRecvd))
			{
				waitLock.notify();
			}
		}
	}
	
	private void runSearches() throws InterruptedException
	{	
		long start = System.currentTimeMillis();
		simulatedTime = MIN_UNIX_TIME;
		while( simulatedTime <= MAX_UNIX_TIME )
		{
			Date date = new Date((long)simulatedTime*1000L); 
						// *1000 is to convert seconds to milliseconds
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); 
						// the format of your date
			sdf.setTimeZone(TimeZone.getTimeZone("GMT")); 
						// give a timezone reference for formating (see comment at the bottom
			String dateFormat = sdf.format(date);
			System.out.println("Current simulated time "+simulatedTime+" time in GMT-5 "
					+dateFormat+" numSent "+numSent+" numRecvd "+numRecvd);
			sendSearchesWhoseTimeHasCome(simulatedTime);
			Thread.sleep(1000);
			simulatedTime = simulatedTime +timeContractionFactor;
			
		}
		long end = System.currentTimeMillis();
		double sendingRate = (numSent*1000.0)/(end-start);
		System.out.println("Eventual sending rate "+sendingRate+" reqs/s");
		this.waitForFinish();
		long endTime = System.currentTimeMillis();
		double systemThpt = (numRecvd*1000.0)/(endTime-start);
		System.out.println("System throughput "+systemThpt+" reqs/s");
		System.out.println("Avg update latency "+(sumSearchLatency/numRecvd)+" ms");
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
			
			String searchQuery
				= "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE "+LAT_ATTR_NAME+" >= "+minLat+
				" AND "+LAT_ATTR_NAME+" <= "+maxLat+" AND "+LONG_ATTR_NAME+" >= "+
				minLong+" AND "+LONG_ATTR_NAME+" <= "+maxLong;
			
			ExperimentSearchReply searchRep 
							= new ExperimentSearchReply( requestId );
			numSent++;
			long queryExpiry = 300000;
			csClient.sendSearchQueryWithCallBack
				( searchQuery, queryExpiry, searchRep, this.getCallBack() );
		}
	}
	
	public static void main( String[] args ) throws NoSuchAlgorithmException, IOException, InterruptedException
	{
		csHost = args[0];
		csPort = Integer.parseInt(args[1]);
		IssueSearches issueSearch = new IssueSearches();
		issueSearch.runSearches();
	}
}