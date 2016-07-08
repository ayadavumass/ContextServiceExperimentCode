package edu.umass.cs.mysqlBenchmarking;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DeleteTask implements Runnable
{
	private final String guid;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public DeleteTask(String guid, AbstractRequestSendingClass requestSendingTask)
	{
		this.guid = guid;
		this.requestSendingTask = requestSendingTask;
	}
	
	@Override
	public void run()
	{
		try 
		{
			long start = System.currentTimeMillis();
			deleteRecord();
			long end = System.currentTimeMillis();
			requestSendingTask.incrementUpdateNumRecvd(guid, end-start);
		} catch (SQLException e) 
		{
			e.printStackTrace();
		}
	}
	
	public void deleteRecord() throws SQLException
	{
		Connection myConn = null;
		Statement statement = null;
		
		try
		{
			myConn = MySQLThroughputBenchmarking.dsInst.getConnection();
			
			String deleteSQL = "DELETE FROM "+MySQLThroughputBenchmarking.tableName 
					+" WHERE nodeGUID=X'"+guid+"'";
			
			statement = (Statement) myConn.createStatement();
			statement.executeUpdate(deleteSQL);
		} catch (SQLException e) 
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				if(statement != null)
					statement.close();
				
				if(myConn != null)
					myConn.close();
			} catch (SQLException e)
			{
				e.printStackTrace();
			}
		}
	}
}