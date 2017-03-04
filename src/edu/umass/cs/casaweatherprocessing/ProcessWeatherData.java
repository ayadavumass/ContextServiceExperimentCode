package edu.umass.cs.casaweatherprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import org.json.JSONException;
import org.json.JSONObject;

public class ProcessWeatherData
{
	public static final String WEATHER_FILE_PATH		 
													= "/home/ayadav/Documents/Data/casaweatherdata/mergedAlerts";
	
	public static final String PER_DAY_ALERT_DIR 
													= "/home/ayadav/Documents/Data/casaweatherdata/perDayAlerts";
	
	
	public static final String PER_DAY_SORTED_DIR
													= "/home/ayadav/Documents/Data/casaweatherdata/perDayAlertsSorted";
	
	
	public static final String PROPERTIES_KEY		= "properties";
	public static final String VALIDAT_KEY			= "validAt";
	public static final String EXPIRE_KEY			= "expires";
	public static final String TIMESTAMP_KEY		= "timestamp";
	public static final String TEXAS_TIMEZONE		= "GMT-6";
	public static final String HAZARD_KEY			= "hazardType";
	
	
	private static HashMap<String, Long> dateHashMap = new HashMap<String, Long>();
	
	private static void computeWeatherDates()
	{
		BufferedReader br = null;
		try
		{
			br = new BufferedReader(new FileReader(WEATHER_FILE_PATH));
			
			String currLine;
			while( (currLine=br.readLine()) != null)
			{
				try
				{
					JSONObject weatherJSON = new JSONObject(currLine);
					JSONObject propJSON = weatherJSON.getJSONObject(PROPERTIES_KEY);
					
					String validAtTimeString = propJSON.getString(VALIDAT_KEY);
					
					String[] parsed = validAtTimeString.split("T");
					String date = parsed[0];
					
					if( dateHashMap.containsKey(date) )
					{
						long currEvent = dateHashMap.get(date);
						currEvent++;
						dateHashMap.put(date, currEvent);
					}
					else
					{
						long currEvent = 1;
						dateHashMap.put(date, currEvent);
					}
				}
				catch (JSONException e) 
				{
					e.printStackTrace();
				}
			}
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
		}
		finally
		{
			if(br != null)
			{
				try 
				{
					br.close();
				}
				catch (IOException e) 
				{
					e.printStackTrace();
				}
			}
		}
		
		Iterator<String> dateIter = dateHashMap.keySet().iterator();
		while( dateIter.hasNext() )
		{
			String date = dateIter.next();
			long events = dateHashMap.get(date);
			System.out.println("date="+date+" events="+events);
		}
	}
	
	
	private static void writePerDayAlerts()
	{
		BufferedReader br = null;
		try
		{
			br = new BufferedReader(new FileReader(WEATHER_FILE_PATH));
			
			String currLine;
			while( (currLine=br.readLine()) != null)
			{
				try
				{
					JSONObject weatherJSON = new JSONObject(currLine);
					JSONObject propJSON = weatherJSON.getJSONObject(PROPERTIES_KEY);
					
					String timeString = "";
					long unixtimestamp;
					if( propJSON.has(TIMESTAMP_KEY) )
					{
						timeString = propJSON.getString(TIMESTAMP_KEY);
						
						String[] parsed = timeString.split("T");
						String date = parsed[0];
						String time = parsed[1].substring(0, parsed[1].length()-1); // excluding Z
						
						String datetime = date+" "+time;
						
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // the format of your date
						sdf.setTimeZone(TimeZone.getTimeZone("GMT")); // give a timezone reference for formating (see comment at the bottom
						unixtimestamp = sdf.parse(datetime).getTime()/1000;
						
					}else
					{
						timeString = propJSON.getString(VALIDAT_KEY);
						
						String[] parsed = timeString.split("T");
						String date = parsed[0];
						String time = parsed[1].substring(0, parsed[1].length()-1); // excluding Z
						
						String datetime = date+" "+time;
						
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // the format of your date
						sdf.setTimeZone(TimeZone.getTimeZone(TEXAS_TIMEZONE)); // give a timezone reference for formating (see comment at the bottom
						unixtimestamp = sdf.parse(datetime).getTime()/1000;
					}
					
					
					Date date = new Date(unixtimestamp*1000); // *1000 is to convert seconds to milliseconds
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // the format of your date
					sdf.setTimeZone(TimeZone.getTimeZone(TEXAS_TIMEZONE)); // give a timezone reference for formating (see comment at the bottom
					String formattedDate = sdf.format(date);
					String onlyDay = formattedDate.split(" ")[0];
					
					
					String writeFName = "Alert"+onlyDay;
					
					BufferedWriter bw = null;
					
					try
					{
						bw = new BufferedWriter(new FileWriter(PER_DAY_ALERT_DIR
													+"/"+writeFName, true));
						
						bw.write(currLine+"\n");
					}
					catch(IOException ioex)
					{
						ioex.printStackTrace();
					}
					finally
					{
						if(bw != null)
						{
							bw.close();
						}
					}
				}
				catch (JSONException e) 
				{
					e.printStackTrace();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
		}
		finally
		{
			if(br != null)
			{
				try 
				{
					br.close();
				}
				catch (IOException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	
	private static void perdaysorted()
	{
		File folder = new File(PER_DAY_ALERT_DIR);
		File[] listOfFiles = folder.listFiles();
		
		for (int i = 0; i < listOfFiles.length; i++)
		{
			if ( listOfFiles[i].isFile() )
			{
				String filename = listOfFiles[i].getName();
				sortAlertFile(filename);
			}
			else if (listOfFiles[i].isDirectory())
			{
				//assert(false);
				System.out.println("Directory " + listOfFiles[i].getName());
			}
		}
	}
	
	private static void sortAlertFile(String filename)
	{
		List<AlertStore> alertList = new LinkedList<AlertStore>();
		
		BufferedReader br = null;
		BufferedWriter bw = null;
		try
		{
			br = new BufferedReader(new FileReader(
									PER_DAY_ALERT_DIR+"/"+filename));
			
			bw = new BufferedWriter(new FileWriter(PER_DAY_SORTED_DIR+"/"+filename));
			
			String currLine; 
			while( (currLine = br.readLine()) != null )
			{
				try
				{
					JSONObject weatherJSON = new JSONObject(currLine);
					JSONObject propJSON = weatherJSON.getJSONObject(PROPERTIES_KEY);
					
					String validAtTimeString = "";
					long unixtimestamp = -1;
					if( propJSON.has(TIMESTAMP_KEY) )
					{
						validAtTimeString = propJSON.getString(TIMESTAMP_KEY);
						
						String[] parsed = validAtTimeString.split("T");
						String date = parsed[0];
						String time = parsed[1].substring(0, parsed[1].length()-1); // excluding Z
						
						String datetime = date+" "+time;
						
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // the format of your date
						sdf.setTimeZone(TimeZone.getTimeZone("GMT")); // give a timezone reference for formating (see comment at the bottom
						unixtimestamp = sdf.parse(datetime).getTime()/1000;
						
					}else
					{
						validAtTimeString = propJSON.getString(VALIDAT_KEY);
						
						String[] parsed = validAtTimeString.split("T");
						String date = parsed[0];
						String time = parsed[1].substring(0, parsed[1].length()-1); // excluding Z
						
						String datetime = date+" "+time;
						
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // the format of your date
						sdf.setTimeZone(TimeZone.getTimeZone(TEXAS_TIMEZONE)); // give a timezone reference for formating (see comment at the bottom
						unixtimestamp = sdf.parse(datetime).getTime()/1000;
					}
					
					
					AlertStore als = new AlertStore();
					als.unixtimestamp = unixtimestamp;
					als.alertjson = weatherJSON;
					
					assert(als != null);
					alertList.add(als);
					
					alertList.sort(new AlertStore());
				}
				catch(JSONException jsonex)
				{
					System.out.println(currLine);
					jsonex.printStackTrace();
					break;
				} catch (ParseException e) 
				{
					e.printStackTrace();
				}	
			}
			
			for(int i=0; i<alertList.size(); i++)
			{
				JSONObject alertJson = alertList.get(i).alertjson;
				bw.write(alertJson.toString()+"\n");
			}
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
		}
		finally
		{
			if( br != null )
			{
				try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if( bw != null )
			{
				try {
					bw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}	
		}
	}
	
	private static class AlertStore implements Comparator<AlertStore>
	{
		long unixtimestamp;
		JSONObject alertjson;	
		
		@Override
		public int compare(AlertStore o1, AlertStore o2) 
		{
			if(o1.unixtimestamp < o2.unixtimestamp)
				return -1;
			else
				return 1;
		}
	}
	
	private static void computePerDayEvents()
	{
		HashMap<String, List<Long>> hazardTypeMap 
										= new HashMap<String, List<Long>>();
		
		List<Long> eventList = new LinkedList<Long>();
		File folder = new File(PER_DAY_SORTED_DIR);
		File[] listOfFiles = folder.listFiles();
		
		for (int i = 0; i < listOfFiles.length; i++)
		{
			if ( listOfFiles[i].isFile() )
			{
				String filename = listOfFiles[i].getName();
				long currEvent = computePerDayEvents(filename, hazardTypeMap);
				System.out.println("filename="+filename+" currEvent="+currEvent);
				eventList.add(currEvent);
			}
			else if (listOfFiles[i].isDirectory())
			{
				//assert(false);
				System.out.println("Directory " + listOfFiles[i].getName());
			}
		}
		eventList.sort(null);
		
		double perc = 1.0;
		for(int i=0; i<eventList.size(); i++)
		{
			System.out.println((perc/eventList.size())+","+eventList.get(i));
			perc++;
		}
		
		Iterator<String> hazardTypeIter = hazardTypeMap.keySet().iterator();
		
		while( hazardTypeIter.hasNext() )
		{
			String hazardName = hazardTypeIter.next();
			List<Long> currNumList = hazardTypeMap.get(hazardName);
			
			
			System.out.println("hazardName="+hazardName+" active days="+currNumList.size()+" "+currNumList.get(0));
		}
		
	}
	
	
	private static long computePerDayEvents(String filename, 
									HashMap<String, List<Long>> hazardTypeMap)
	{
		HashMap<String, Long> localHazardMap = new HashMap<String, Long>();
		
		BufferedReader br = null;
		long currEvent = 0;
		try
		{
			br = new BufferedReader(new FileReader(
									PER_DAY_ALERT_DIR+"/"+filename));
			
			String currLine; 
			while( (currLine = br.readLine()) != null )
			{
				currEvent++;
				
				try
				{
					JSONObject json = new JSONObject(currLine);
					
					if( json.has(HAZARD_KEY) )
					{
						String hazardType = json.getString(HAZARD_KEY);
						
						if( localHazardMap.containsKey(hazardType) )
						{
							long currNum = localHazardMap.get(hazardType);
							currNum++;
							localHazardMap.put(hazardType, currNum);
						}
						else
						{
							long currNum = 1;
							localHazardMap.put(hazardType, currNum);
						}
					}
				}
				catch (JSONException e) 
				{
					e.printStackTrace();
				}
			}
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
		}
		finally
		{
			if( br != null )
			{
				try 
				{
					br.close();
				} 
				catch (IOException e) 
				{
					e.printStackTrace();
				}
			}
		}
		
		Iterator<String> hazardTypeIter = localHazardMap.keySet().iterator();
		
		while( hazardTypeIter.hasNext() )
		{
			String hazardName = hazardTypeIter.next();
			long currNum = localHazardMap.get(hazardName);
			List<Long> countList = hazardTypeMap.get(hazardName);
			
			if(countList == null)
			{
				countList = new LinkedList<Long>();
				countList.add(currNum);
				hazardTypeMap.put(hazardName, countList);
			}
			else
			{
				countList.add(currNum);
			}
		}
		return currEvent;
	}
	
	public static void main(String[] args)
	{
		computeWeatherDates();
		//writePerDayAlerts();
		//perdaysorted();
		//computePerDayEvents();
	}
}