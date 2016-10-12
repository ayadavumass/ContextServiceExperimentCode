package edu.umass.cs.nomadlogProcessing;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Stores each log entry
 * @author adipc
 */
public class LogEntryClass 
{
	private final long unixTimestamp;
	private final double latitude;
	private final double longitude;
	private final String dateFormat;
	
	public LogEntryClass(long unixTimestamp, double latitude, double longitude)
	{
		this.unixTimestamp = unixTimestamp;
		this.latitude = latitude;
		this.longitude = longitude;
		
		Date date = new Date(unixTimestamp*1000L); // *1000 is to convert seconds to milliseconds
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); // the format of your date
		sdf.setTimeZone(TimeZone.getTimeZone("GMT-5")); // give a timezone reference for formating (see comment at the bottom
		dateFormat = sdf.format(date);
	}
	
	public Long getUnixTimeStamp()
	{
		return unixTimestamp;
	}
	
	public double getLatitude()
	{
		return this.latitude;
	}
	
	public double getLongitude()
	{
		return this.longitude;
	}
	
	public String getDateString()
	{
		return dateFormat;
	}
}