package edu.umass.cs.mysqlBenchmarking;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class InitializeTask implements Runnable
{
	private final String guid;
	private final double value1;
	private final double value2;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public InitializeTask(String guid, double value1, double value2,
			AbstractRequestSendingClass requestSendingTask)
	{
		this.guid = guid;
		this.value1 = value1;
		this.value2 = value2;
		this.requestSendingTask = requestSendingTask;
	}
	
	@Override
	public void run()
	{
		
		try 
		{
			long start = System.currentTimeMillis();
			putValueObjectRecord();
			long end = System.currentTimeMillis();
			requestSendingTask.incrementUpdateNumRecvd(guid, end-start);
		} catch (SQLException e) 
		{
			e.printStackTrace();
		}
		
	}
	
	public void putValueObjectRecord() throws SQLException
	{
		Connection myConn = MySQLBenchmarking.dsInst.getConnection();
		Statement statement = null;

		String insertTableSQL = "INSERT INTO "+MySQLBenchmarking.tableName 
				+" (value1, value2, nodeGUID, versionNum) " + "VALUES"
				+ "("+value1+","+value2+",'"+guid+"',"+-1 +")";

		try 
		{
			statement = (Statement) myConn.createStatement();
			
			statement.executeUpdate(insertTableSQL);
		} catch (SQLException e) 
		{
			e.printStackTrace();
		} finally
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
