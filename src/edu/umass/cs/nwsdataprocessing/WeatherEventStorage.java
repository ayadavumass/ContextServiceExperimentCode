package edu.umass.cs.nwsdataprocessing;

import java.util.List;

import edu.umass.cs.acs.geodesy.GlobalCoordinate;

public class WeatherEventStorage
{
	//"WFO" field in the data.
	private final String issuingRadarId;
	
	private final String issueTime;
	private final String expireTime;
	private final String weatherPheCode;
	
	// time stamp is in seconds
	private final long issueUnixTimeStamp;
	private final long expireUnixTimeStamp;
	
	// area km2 of the warning, "AREA_KM2" field 
	private final double areaKm2;
	
	private List<List<GlobalCoordinate>> listOfPolygons;
	
	public WeatherEventStorage( String issuingRadarId, String issueTime, String expireTime, 
			String weatherPheCode, long issueUnixTimeStamp, long expireUnixTimeStamp,
			List<List<GlobalCoordinate>> listOfPolygons, double areaKm2)
	{
		this.issuingRadarId = issuingRadarId;
		this.issueTime = issueTime;
		this.expireTime = expireTime;
		this.weatherPheCode = weatherPheCode;
		this.issueUnixTimeStamp = issueUnixTimeStamp;
		this.expireUnixTimeStamp = expireUnixTimeStamp;
		this.listOfPolygons = listOfPolygons;
		this.areaKm2 = areaKm2;
	}
	
	public String getIssuingRadarId()
	{
		return this.issuingRadarId;
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
	
	public Long getIssueUnixTimeStamp()
	{
		return issueUnixTimeStamp;
	}
	
	public long getExpireUnixTimeStamp()
	{
		return this.expireUnixTimeStamp;
	}
	
	public List<List<GlobalCoordinate>> getListOfPolygons()
	{
		return this.listOfPolygons;
	}
	
	public double getAreaKm2()
	{
		return this.areaKm2;
	}
	
	public double getDurationInSecs()
	{
		double dur = expireUnixTimeStamp - issueUnixTimeStamp;
		return dur;
	}
}