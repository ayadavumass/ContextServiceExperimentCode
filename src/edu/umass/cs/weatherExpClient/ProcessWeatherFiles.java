package edu.umass.cs.weatherExpClient;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;


public class ProcessWeatherFiles
{
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
    }
	 */
	public static final String weatherDirName = "/home/adipc/Documents/MobilityFirstGitHub/Alert-Control-System/data/ALL-12-26";
	
	public static void main(String[] args)
	{
		readWeatherFiles();
		//calculateWeatherFilesWithLargerExpiration();
	}
	
	public static void calculateWeatherFilesWithLargerExpiration()
	{
		File folder = new File(weatherDirName);
		File[] listOfFiles = folder.listFiles();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		UnixTimeDiffStorageClass[] dateArray = new UnixTimeDiffStorageClass[listOfFiles.length];
		
		//Date beginTimestamp = Date parse = sdf.parse(timestamp);
		int count = 0;
		for (File file : listOfFiles) 
		{
			if (file.isFile()) 
			{
				//System.out.println(file.getName());
				JSONObject fileJSON = readFileAndConjureAJSON(file.getName());
				if(fileJSON.has("properties"))
				{
					try
					{
						JSONObject propertiesJSON = fileJSON.getJSONObject("properties");
						
						Date validTime  = null;
						Date expireTime = null;
						if(propertiesJSON.has("validAt"))
						{
							String timestamp = propertiesJSON.getString("validAt");
							timestamp = timestamp.substring(0, timestamp.length()-3);
							
							//System.out.println("Alert expiry "+timestamp);
							validTime = sdf.parse(timestamp);
							System.out.println(count+" parsed date "+validTime.toString()
									+" timestamp "+timestamp);
							
							//dateArray[count] = new DateStorageClass(parse, file.getName());
							//count++;
						}
						
						if( propertiesJSON.has("expires") )
						{
							String timestamp = propertiesJSON.getString("expires");
							timestamp = timestamp.substring(0, timestamp.length()-3);
							
							//System.out.println("Alert expiry "+timestamp);
							expireTime = sdf.parse(timestamp);
							System.out.println(count+" parsed date "+expireTime.toString()
									+" timestamp "+timestamp);
							
							//dateArray[count] = new DateStorageClass(parse, file.getName());
							//count++;
						}
						
						if((expireTime != null) && (validTime != null) )
						{
							long expUnixTime   = expireTime.getTime();
							long validUnixTime = validTime.getTime();
							dateArray[count] = new UnixTimeDiffStorageClass
									((expUnixTime-validUnixTime), file.getName());
							count++;
						}
					} catch (JSONException e) 
					{
						e.printStackTrace();
					} catch (ParseException e) 
					{
						e.printStackTrace();
					}
				}
				
			}
		}
		Arrays.sort(dateArray);
		//System.out.println("first element "+dateArray[0].toString()+" second element "+dateArray[1].toString());
		
		//  begin timestamp 2015-12-26T20:03:23
		// end timestamp 2015-12-26T23:00:13
		
		
		try 
		{
			//Date traceBeginDate = sdf.parse("2015-12-26T20:03:23");
			//Date traceEndDate   = sdf.parse("2015-12-26T23:00:13");
			
			File file = new File("DurationSorted.txt");
			// if file doesnt exists, then create it
			if ( !file.exists() )
			{
				file.createNewFile();
			}
			
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			
			for(int i=0;i<dateArray.length;i++)
			{
				UnixTimeDiffStorageClass dateStorage = dateArray[i];
				
//				if( (dateStorage.getDate().after(traceBeginDate) || 
//						dateStorage.getDate().equals(traceBeginDate)) && 
//						( dateStorage.getDate().before(traceEndDate) || 
//								dateStorage.getDate().equals(traceEndDate) ) )
				{
					
					bw.write("Duration "+dateArray[i].getUnixTimeDiff()
							+" filename "+dateArray[i].getFileName()+"\n");
					
					System.out.println("Duration "+dateArray[i].getUnixTimeDiff()
							+" filename "+dateArray[i].getFileName());
					
				}
			}
			bw.close();
		} catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	private static void readWeatherFiles()
	{
		File folder = new File(weatherDirName);
		File[] listOfFiles = folder.listFiles();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		DateStorageClass[] dateArray = new DateStorageClass[listOfFiles.length];
		
		//Date beginTimestamp = Date parse = sdf.parse(timestamp);
		int count = 0;
		for (File file : listOfFiles) 
		{
			if (file.isFile()) 
			{
				//System.out.println(file.getName());
				JSONObject fileJSON = readFileAndConjureAJSON(file.getName());
				if(fileJSON.has("properties"))
				{
					try
					{
						JSONObject propertiesJSON = fileJSON.getJSONObject("properties");
						
						if(propertiesJSON.has("validAt"))
						{
							String timestamp = propertiesJSON.getString("validAt");
							timestamp = timestamp.substring(0, timestamp.length()-3);
							
							//System.out.println("Alert expiry "+timestamp);
							Date parse = sdf.parse(timestamp);
							System.out.println(count+" parsed date "+parse.toString()
									+" timestamp "+timestamp);
							
							Date validDate = parse;
							Date expiryDate = null;
							if(propertiesJSON.has("expires"))
							{
								String expiresTimestamp = propertiesJSON.getString("expires");
								expiresTimestamp = expiresTimestamp.substring(0, expiresTimestamp.length()-3);
								
								//System.out.println("Alert expiry "+timestamp);
								expiryDate = sdf.parse(expiresTimestamp);
							}
							
							dateArray[count] = new DateStorageClass(parse, file.getName(), expiryDate);
							count++;
						}
					} catch (JSONException e) 
					{
						e.printStackTrace();
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
				
			}
		}
		Arrays.sort(dateArray);
		//System.out.println("first element "+dateArray[0].toString()+" second element "+dateArray[1].toString());
		
		//  begin timestamp 2015-12-26T20:03:23
		// end timestamp 2015-12-26T23:00:13
		
		try 
		{
			Date traceBeginDate = sdf.parse("2015-12-26T15:00:00");
			Date traceEndDate   = sdf.parse("2015-12-26T23:00:13");
			
			File file = new File("weather8HourTrace.txt");
			// if file doesnt exists, then create it
			if ( !file.exists() )
			{
				file.createNewFile();
			}
			
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			
			for(int i=0;i<dateArray.length;i++)
			{
				DateStorageClass dateStorage = dateArray[i];
				
				if( (dateStorage.getValidDate().after(traceBeginDate) || 
						dateStorage.getValidDate().equals(traceBeginDate)) && 
						( dateStorage.getValidDate().before(traceEndDate) || 
								dateStorage.getValidDate().equals(traceEndDate) ) )
				{
					
					bw.write(dateStorage.getFileName()+" , duration "+dateStorage.getUnixTimestampDiff()+"\n");
					
					//System.out.println("sorted date "+dateArray[i].getDate().toString());
					
				}
			}
			bw.close();
		} catch (ParseException e) 
		{
			e.printStackTrace();
		} catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	private static class DateStorageClass implements Comparable<DateStorageClass>
	{
		private final Date validDate;
		private final String fileName;
		private final Date expiryDate;
		public DateStorageClass(Date validDate, String fileName, Date expiryDate)
		{
			this.fileName = fileName;
			this.validDate = validDate;
			this.expiryDate = expiryDate;
		}
		
		@Override
		public int compareTo(DateStorageClass compareDate) 
		{
			if(validDate.before(compareDate.getValidDate()))
				return -1;
			else if(validDate.after(compareDate.getValidDate()))
				return 1;
			else
				return 0;
		}
		
		public String getFileName()
		{
			return this.fileName;
		}
		
		public Date getValidDate()
		{
			return this.validDate;
		}
		
		public Date getExpiryDate()
		{
			return this.expiryDate;
		}
		
		public long getUnixTimestampDiff()
		{
			return (expiryDate.getTime()-validDate.getTime())/1000;
		}
	}
	
	
	private static class UnixTimeDiffStorageClass implements Comparable<UnixTimeDiffStorageClass>
	{
		private final long unixTimeDiff;
		private final String fileName;
		
		public UnixTimeDiffStorageClass(long unixTimeDiff, String fileName)
		{
			this.unixTimeDiff = unixTimeDiff;
			this.fileName = fileName;
		}
		
		@Override
		public int compareTo(UnixTimeDiffStorageClass compareDate) 
		{
			if(unixTimeDiff < compareDate.getUnixTimeDiff())
				return -1;
			else if(unixTimeDiff > compareDate.getUnixTimeDiff())
				return 1;
			else
				return 0;
		}
		
		public long getUnixTimeDiff()
		{
			return this.unixTimeDiff;
		}
		
		public String getFileName()
		{
			return this.fileName;
		}
	}
	
	private static JSONObject readFileAndConjureAJSON(String fileName)
	{
		BufferedReader br = null;
		String jsonString = "";
		try
		{
			String sCurrentLine;
			br = new BufferedReader(new FileReader(weatherDirName+"/"+fileName));
			
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