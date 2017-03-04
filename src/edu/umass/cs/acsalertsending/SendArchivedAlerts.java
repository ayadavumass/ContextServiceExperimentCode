package edu.umass.cs.acsalertsending;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.json.JSONException;
import org.json.JSONObject;


public class SendArchivedAlerts
{
	//public static final String ARCHIVED_ALERT_DIR 			= "/home/ayadav/Documents/Data/casaweatherdata/perDayAlertsSorted";
	public static final String ARCHIVED_ALERT_DIR 				= "/home/ec2-user/perDayAlertsSorted";
	
	public static final String ACS_HOST_IP						= "127.0.0.1";
	public static final int ACS_LISTEN_PORT						= 30986;
	
	// ACS header pattern
	public static final String HEADER_PATTERN 					= "&&&"; // Could be an arbitrary string
	
	public static final String TIME_FORMAT						= "yyyy-MM-dd HH:mm:ss";
	
	public static final String TEXAS_TIMEZONE					= "GMT-6";
	
	public static final long SLEEP_TIME							= 1000;
	
	public static final String VALIDAT_KEY						= "validAt";
	public static final String TIMESTAMP_KEY					= "timestamp";
	
	// clock variable. TimeThread class updates it every sec.
	private static long currUnixTimestamp;
	
	private static final Object TIME_TICK_LOCK					= new Object();
	
	
	private static void sendAlertsToACS()
	{
		Date currDate = new Date(currUnixTimestamp*1000);
		SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);
		sdf.setTimeZone(TimeZone.getTimeZone(TEXAS_TIMEZONE));
		String format = sdf.format(currDate);
		
		String onlyDay = format.split(" ")[0];
		
		String filename = "Alert"+onlyDay;
		
		
		BufferedReader br = null;
		try
		{
			br = new BufferedReader( new FileReader(
						ARCHIVED_ALERT_DIR+"/"+filename) );
			
			while(true)
			{
				String currLine = null;
				JSONObject alertJSON = null;
				long alertTimeStamp = -1;
				
				while( (currLine=br.readLine()) != null )
				{
					try
					{
						alertJSON = new JSONObject(currLine);
						
						if( alertJSON.has(TIMESTAMP_KEY) )
						{
							String issueString = alertJSON.getString(TIMESTAMP_KEY);
							alertTimeStamp = getUnixtimestampFromLogTimestamp(issueString);	
						}
						else if(alertJSON.has(VALIDAT_KEY))
						{
							String issueString = alertJSON.getString(VALIDAT_KEY);
							alertTimeStamp = getUnixtimestampFromLogValidAtTime(issueString);	
						}
						else
						{
							assert(false);
						}
						
						if(currUnixTimestamp < alertTimeStamp)
						{
							break;
						}
						else
						{
							sendAlert(alertJSON);
						}
					}
					catch (JSONException e) 
					{
						e.printStackTrace();
					}
				}
				
				if(currLine == null)
				{
					// FIXME: file finished, need start alerts from another file.
					System.out.println("Current weather alert file "+filename+" ended");
				}
				else
				{
					assert(alertTimeStamp != -1);
					
					synchronized(TIME_TICK_LOCK)
					{
						while(currUnixTimestamp < alertTimeStamp)
						{
							try 
							{
								TIME_TICK_LOCK.wait();
							} 
							catch (InterruptedException e) 
							{
								e.printStackTrace();
							}
						}
					}
				}	
				// send the read alerts
				sendAlert(alertJSON);
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
	
	private static void sendAlert(JSONObject  jsonobject) throws IOException 
	{
		Socket socket = null;
		
	    socket = new Socket(ACS_HOST_IP, ACS_LISTEN_PORT);
	    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
	    
	    StringBuilder outputBuffer = new StringBuilder();
	    outputBuffer.append(jsonobject.toString());
	    String outputString = outputBuffer.toString();
	    // Basically does "&&&" + str.length() + "&&&" + str
	    outputString = prependHeader(outputString);
	    out.print(outputString);
	    out.flush();
	    System.out.println("Alert sent");
	    out.close();
	    socket.close();
	}
	
	private static void sendTestPreamble()
	{
		try
		{
			Socket socket = null;
			socket = new Socket(ACS_HOST_IP, ACS_LISTEN_PORT);
			
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			JSONObject json = new JSONObject();
			json.put("simulateTime", "true");
			String outputString = json.toString();
			// Basically does "&&&" + str.length() + "&&&" + str
			
			outputString = prependHeader(outputString);
			out.print(outputString);
			out.flush();
			
			System.out.println("Preamble sent");
			out.close();
			socket.close();
		} 
		catch (IOException | JSONException e)
		{
			e.printStackTrace();
	    }
	}
	
	
	/*
	 * Header is of the form pattern<size>pattern. The pattern is
	 * changeable above. But there is no escape character support
	 * built into the data itself. So if the header pattern occurs
	 * in the data, we could lose a ***lot*** of data, especially
	 * if the bad "size" happens to be a huge value. A big bad size
	 * also means a huge of amount of buffering in SockStreams.
	 */
	private static String prependHeader(String str) 
	{
		return (HEADER_PATTERN + str.length()+ HEADER_PATTERN + str);
	}
	
	private static long getUnixtimestampFromLogTimestamp(String datetimeString)
	{	
		try 
		{
			String[] dateTime = datetimeString.split("T");
			String onlyDay = dateTime[0];
			
			// removing Z
			String timeString = dateTime[1].substring(0, dateTime[1].length()-1);
			
			SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);
			sdf.setTimeZone(TimeZone.getTimeZone(TEXAS_TIMEZONE));
			
			long unixtimestamp = sdf.parse(onlyDay+" "+timeString).getTime()/1000;
			
			return unixtimestamp;
		}
		catch (ParseException e) 
		{
			e.printStackTrace();
		}
		return -1;
	}
	
	private static long getUnixtimestampFromLogValidAtTime(String datetimeString)
	{
		try 
		{
			String[] dateTime = datetimeString.split("T");
			String onlyDay = dateTime[0];
			
			// removing Z
			String timeString = dateTime[1].split("-")[0];
			
			SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);
			sdf.setTimeZone(TimeZone.getTimeZone(TEXAS_TIMEZONE));
			
			long unixtimestamp = sdf.parse(onlyDay+" "+timeString).getTime()/1000;
			
			return unixtimestamp;
		}
		catch (ParseException e) 
		{
			e.printStackTrace();
		}
		return -1;
	}
	
	
	private static class TimeThread implements Runnable
	{		
		public TimeThread(String startDateTime)
		{		
			try
			{
				SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT); 
				sdf.setTimeZone(TimeZone.getTimeZone(TEXAS_TIMEZONE)); 
				currUnixTimestamp = sdf.parse(startDateTime).getTime()/1000;
			}
			catch (ParseException e) 
			{
				e.printStackTrace();
			}
		}
		
		@Override
		public void run()
		{
			while(true)
			{
				try
				{
					Thread.sleep(SLEEP_TIME);
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
				
				currUnixTimestamp = currUnixTimestamp + 1;
				
				synchronized(TIME_TICK_LOCK)
				{
					TIME_TICK_LOCK.notify();
				}
			}
		}
	}
	
	
	public static void main(String[] args)
	{
		// this is the start date time when the alert start
		String startDateTime = args[0];
		
		TimeThread timeThread = new TimeThread(startDateTime);
		sendTestPreamble();
		System.out.println("Preamble sent");
		new Thread(timeThread).start();
		sendAlertsToACS();
	}
}