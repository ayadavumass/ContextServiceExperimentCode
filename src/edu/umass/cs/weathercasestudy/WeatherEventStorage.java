package edu.umass.cs.weathercasestudy;

import java.util.List;

import edu.umass.cs.acs.geodesy.GlobalCoordinate;

public class WeatherEventStorage
{
	private final long weatherEventId;
	private final String issueTime;
	private final String expireTime;
	private final String weatherPheCode;
	
	// time stamp is in seconds
	private final long issueUnixTimeStamp;
	private final long expireUnixTimeStamp;
	
	private List<List<GlobalCoordinate>> listOfPolygons;
	
	public WeatherEventStorage( long weatherEventId, String issueTime, String expireTime, 
			String weatherPheCode, List<List<GlobalCoordinate>> listOfPolygons, 
			long issueUnixTimeStamp, long expireUnixTimeStamp )
	{
		this.weatherEventId = weatherEventId;
		this.issueTime = issueTime;
		this.expireTime = expireTime;
		this.weatherPheCode = weatherPheCode;
		this.listOfPolygons = listOfPolygons;
		this.issueUnixTimeStamp = issueUnixTimeStamp;
		this.expireUnixTimeStamp = expireUnixTimeStamp;
	}
	
	public long getWeatherEventId()
	{
		return this.weatherEventId;
	}
	
	public String getIssueTime()
	{
		return this.issueTime;
	}
	
	public String getExpireTime()
	{
		return this.expireTime;
	}
	
	public String getWeatherPheCode()
	{
		return this.weatherPheCode;
	}
	
	public List<List<GlobalCoordinate>> getListOfPolygons()
	{
		return this.listOfPolygons;
	}
	
	public Long getIssueUnixTimeStamp()
	{
		return issueUnixTimeStamp;
	}
	
	public long getExpireUnixTimeStamp()
	{
		return this.expireUnixTimeStamp;
	}
}