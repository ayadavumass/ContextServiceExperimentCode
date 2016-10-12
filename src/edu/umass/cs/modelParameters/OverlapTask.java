package edu.umass.cs.modelParameters;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OverlapTask implements Runnable
{
	private final String searchQuery;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public OverlapTask( String searchQuery,
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
			long start = System.currentTimeMillis();
			int replySize = getOverlappingPartitions();
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
	
	public int getOverlappingPartitions()
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
				//hashCode, respNodeID
				String respNodeId = rs.getString("respNodeID");
				resultSize++;
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