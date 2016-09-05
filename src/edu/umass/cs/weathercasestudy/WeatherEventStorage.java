package edu.umass.cs.weathercasestudy;

import java.util.List;

import edu.umass.cs.acs.geodesy.GlobalCoordinate;

public class WeatherEventStorage
{
	private final String issuedTime;
	private final String finalTime;
	private final String weatherPheCode;
	
	// time stamp is in seconds
	private final long issueUnixTimeStamp;
	
	private List<List<GlobalCoordinate>> listOfPolygons;
	
	public WeatherEventStorage( String issuedTime, String finalTime, 
			String weatherPheCode, List<List<GlobalCoordinate>> listOfPolygons, 
			long issueUnixTimeStamp )
	{
		this.issuedTime = issuedTime;
		this.finalTime = finalTime;
		this.weatherPheCode = weatherPheCode;
		this.listOfPolygons = listOfPolygons;
		this.issueUnixTimeStamp = issueUnixTimeStamp;
	}
	
	public String getIssueTime()
	{
		return this.issuedTime;
	}
	
	public String getFinalTime()
	{
		return this.finalTime;
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
}