package edu.umass.cs.contextservicebenchmarking;

import java.sql.SQLException;

import org.json.JSONObject;

public class InsertTask implements Runnable
{
	private final String guidAlias;
	private final double latitude;
	private final double longitude;
	
	//private final JSONObject attrValJSON;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public InsertTask( String guidAlias, double latitude, double longitude, 
			AbstractRequestSendingClass requestSendingTask )
	{
		this.guidAlias = guidAlias;
		this.latitude  = latitude;
		this.longitude = longitude;
		this.requestSendingTask = requestSendingTask;
	}
	
	@Override
	public void run()
	{
		try
		{
			long start = System.currentTimeMillis();
			insertGUIDRecord();
			long end = System.currentTimeMillis();
			requestSendingTask.incrementUpdateNumRecvd(guidAlias, end-start);
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
	
	public void insertGUIDRecord() throws SQLException
	{
		try 
		{
			String guid = SelectCallBenchmarking.getSHA1(this.guidAlias);
			JSONObject attrValJSON = new JSONObject();
			attrValJSON.put("latitude", latitude);
			attrValJSON.put("longitude", longitude);
			
			SelectCallBenchmarking.csClient.sendUpdate( guid, null, attrValJSON, -1 );
			//setLocation(guidEntry, longitude, latitude);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}