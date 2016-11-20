package edu.umass.cs.sqliteBenchmarking;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class GetTask implements Runnable
{
	private final String guid;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public GetTask(String guid, AbstractRequestSendingClass requestSendingTask)
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
			getGUIDRecord();
			long end = System.currentTimeMillis();
			requestSendingTask.incrementUpdateNumRecvd(guid, end-start);
		} catch (SQLException e) 
		{
			e.printStackTrace();
		}
	}
	
	public void getGUIDRecord() throws SQLException
	{
		Connection myConn = null;
		Statement statement = null;
		
		try
		{	
			String selectTableSQL = "SELECT * FROM "
					+ SQLiteThroughputBenchmarking.dataTableName 
					+ " WHERE nodeGUID = X'"+guid+"'";
			
			myConn = SQLiteThroughputBenchmarking.dsInst.getConnection();
			statement = (Statement) myConn.createStatement();
			ResultSet rs = statement.executeQuery(selectTableSQL);
			int numEntries = 0;
			
			while(rs.next())
			{
				numEntries++;
			}
			assert(numEntries == 1);
		}
		catch (SQLException e) 
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
			} 
			catch (SQLException e)
			{
				e.printStackTrace();
			}
		}
	}
}