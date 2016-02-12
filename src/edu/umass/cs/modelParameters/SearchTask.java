package edu.umass.cs.modelParameters;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.json.JSONArray;

public class SearchTask implements Runnable
{
	private final String searchQuery;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public SearchTask( String searchQuery,
			AbstractRequestSendingClass requestSendingTask )
	{
		this.searchQuery = searchQuery;
		this.requestSendingTask = requestSendingTask;
	}
	
	@Override
	public void run()
	{
		try
		{
			JSONArray resultArray = new JSONArray();
			long start = System.currentTimeMillis();
			int replySize = getValueInfoObjectRecord(resultArray);
			long end = System.currentTimeMillis();
			requestSendingTask.incrementSearchNumRecvd(replySize, end-start);
		} catch(Exception ex)
		{
			ex.printStackTrace();
		}
		catch(Error ex)
		{
			ex.printStackTrace();
		}
	}
	
	public int getValueInfoObjectRecord(JSONArray resultArray)
	{
		Connection myConn = null;
		Statement stmt 	  = null;
		int resultSize 	  = 0;
		
		try
		{
			myConn = ThroughputMeasure.dsInst.getConnection();
			stmt = myConn.createStatement();
			
			ResultSet rs = stmt.executeQuery(searchQuery);
			
			while( rs.next() )
			{
				String nodeGUID = rs.getString("nodeGUID");
				resultSize++;
				//jsoArray.put(nodeGUID);
			}
			rs.close();
		} catch(SQLException sqlex)
		{
			sqlex.printStackTrace();
		}
		 finally
		{
			 try 
			{
				if(stmt != null)
					stmt.close();
					
				if(myConn != null)
					myConn.close();
			} catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
		return resultSize;
	}
}