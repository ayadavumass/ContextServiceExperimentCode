package edu.umass.cs.mysqlBenchmarking;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TriggerSearchTask implements Runnable
{
	private final String insertQuery;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public TriggerSearchTask( String insertQuery, 
			AbstractRequestSendingClass requestSendingTask )
	{
		this.insertQuery = insertQuery;
		this.requestSendingTask = requestSendingTask;
	}
	
	@Override
	public void run()
	{
		Connection myConn = null;
		Statement stmt = null;
		
		try
		{	
			myConn = MySQLThroughputBenchmarking.dsInst.getConnection();
			stmt = myConn.createStatement();
			
			long start = System.currentTimeMillis();
			stmt.executeUpdate(insertQuery);
			long end = System.currentTimeMillis();
			
			requestSendingTask.incrementUpdateNumRecvd("", end-start);
			//ncrementSearchNumRecvd(replySize, end-start);
		} 
		catch(SQLException sqlex)
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
			}
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
	}
}