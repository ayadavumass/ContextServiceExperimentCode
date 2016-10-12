package edu.umass.cs.weatherExpClient;

import java.io.BufferedReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Updates locations of all users after every 
 * granularityOfGeolocationUpdate
 * @author adipc
 */
public class WeatherQueryClass extends AbstractRequestSendingClass implements Runnable
{
	// geoJSON array, so that we don't have to read from file each time.
	private Vector<JSONObject> weatherAlertsArray;
	private final Random queryRand;
	
	public WeatherQueryClass()
	{
		super(WeatherAndMobilityBoth.SEARCH_LOSS_TOLERANCE);
		weatherAlertsArray = new Vector<JSONObject>();
		queryRand = new Random(WeatherAndMobilityBoth.myID);
		System.out.println("reading weather files");
		readWeatherFiles();
		System.out.println("reading weather files complete");
	}
	
	@Override
	public void run()
	{
		try
		{
			this.startExpTime();
			weatherQueryRateControlledRequestSender();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	//String query 
	// = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap(geoLocationCurrentLat, geoLocationCurrentLong, "+geoJSONObject.toString()+")";
	private void weatherQueryRateControlledRequestSender() throws Exception
	{	
		// as it is per ms
		double reqspms = WeatherAndMobilityBoth.searchQueryRate/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double currUserGuidNum   = 0;
		
		while( ( (System.currentTimeMillis() - expStartTime) < WeatherAndMobilityBoth.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				JSONObject queryGeoJSON = 
						weatherAlertsArray.get( queryRand.nextInt(weatherAlertsArray.size() ) );
				sendQueryMessage(queryGeoJSON);
				numSent++;
			}
			currTime = System.currentTimeMillis();
			
			double timeElapsed = ((currTime- expStartTime)*1.0);
			double numberShouldBeSentByNow = timeElapsed*reqspms;
			double needsToBeSentBeforeSleep = numberShouldBeSentByNow - numSent;
			if(needsToBeSentBeforeSleep > 0)
			{
				needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
			}
			
			for(int i=0;i<needsToBeSentBeforeSleep;i++)
			{
				JSONObject queryGeoJSON = 
						weatherAlertsArray.get( queryRand.nextInt(weatherAlertsArray.size() ) );
				sendQueryMessage(queryGeoJSON);
				numSent++;
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Search eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Search result:Goodput "+sysThrput);
	}
	
	private void sendQueryMessage(JSONObject queryGeoJSON)
	{
		String searchQuery
			= "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap(geoLocationCurrentLat, geoLocationCurrentLong, "+queryGeoJSON.toString()+")";
		SearchTask searchTask = new SearchTask( searchQuery, new JSONArray(), this );
		WeatherAndMobilityBoth.taskES.execute(searchTask);
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
			System.out.println("Search reply recvd size "+resultSize+" time taken "+timeTaken+
					" numSent "+numSent+" numRecvd "+numRecvd);
			//if(currNumReplyRecvd == currNumReqSent)
			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				waitLock.notify();
			}
		}
	}
	
	private void readWeatherFiles()
	{
		File folder = new File(WeatherAndMobilityBoth.weatherDirName);
		File[] listOfFiles = folder.listFiles();
		
		int count = 0;
		
		for (File file : listOfFiles)
		{
			if ( file.isFile() )
			{
				JSONObject fileJSON = readFileAndConjureAJSON(file.getName());
				
				if( fileJSON.has("geometry") )
				{
					JSONObject geoJSON;
					try 
					{
						geoJSON = fileJSON.getJSONObject("geometry");
						weatherAlertsArray.add(geoJSON);
					} catch (JSONException e) 
					{
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	private static JSONObject readFileAndConjureAJSON(String fileName)
	{
		BufferedReader br = null;
		String jsonString = "";
		try
		{
			String sCurrentLine;
			br = new BufferedReader(new FileReader(WeatherAndMobilityBoth.weatherDirName+"/"+fileName));
			
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
}