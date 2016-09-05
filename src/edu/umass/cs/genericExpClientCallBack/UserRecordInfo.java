package edu.umass.cs.genericExpClientCallBack;

import edu.umass.cs.gnsclient.client.util.GuidEntry;

/**
 * used to store user record
 * @author adipc
 */
public class UserRecordInfo
{
	private String userAlias;
	private GuidEntry userGuidEntry;
	private long lastTimeOfLocationUpdate;
	
	private String guidString;
	
	private double currLatitude;
	private double currLongitude;
	
	private int userActivity;
	
	private double angleOfMovement;
	
	public UserRecordInfo( String userAlias, 
			GuidEntry userGuidEntry, String userGUID )
	{
		this.userAlias = userAlias;
		this.userGuidEntry = userGuidEntry;
		this.guidString = userGUID;
	}
	
	public void updateLocationUpdateTime()
	{
		this.lastTimeOfLocationUpdate = System.currentTimeMillis();
	}
	
	public void setGeoLocation(double latitude, double longitude)
	{
		this.currLatitude = latitude;
		this.currLongitude = longitude;
	}
	
	public double getLatitude()
	{
		return this.currLatitude;
	}
	
	public double getLongitude()
	{
		return this.currLongitude;
	}
	
	public void setUserActivity(int userActivity)
	{
		this.userActivity = userActivity;
	}
	
	public int getUserActivity()
	{
		return this.userActivity;
	}
	
	public void setAngleOfMovement(double angleOfMovement)
	{
		this.angleOfMovement = angleOfMovement;
	}
	
	public double getAngleOfMovement()
	{
		return this.angleOfMovement;
	}
	
	public GuidEntry getUserGuidEntry()
	{
		return this.userGuidEntry;
	}
	
	public String getGUIDString()
	{
		return this.guidString;
	}
}