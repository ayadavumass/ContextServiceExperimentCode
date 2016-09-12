package edu.umass.cs.ubercasestudy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;


public class TaxiDataProcessing 
{
	public static final String TAXI_DATA_PATH		
					= "/home/adipc/Documents/NYCTaxiData/yellow_tripdata_2016-02.csv";
	
	private long currLineNum;
	
	private HashMap<String, Long> perDayTaxiPickUps;
	
	public TaxiDataProcessing()
	{
		currLineNum = 0;
		perDayTaxiPickUps = new HashMap<String, Long>();
	}
	
	public void processData()
	{
		
	}
	
	/*public void readTheTaxiFile()
	{
		BufferedReader br = null;
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader(new FileReader(TAXI_DATA_PATH));
			
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
	}*/
	
	public static void main(String[] args)
	{	
	}
}