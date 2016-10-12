package edu.umass.cs.modelParameters;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Class implements the task used for 
 * update info in GNS, which is blocking so this 
 * class's object is passed in executor service
 * @author adipc
 */
public class UpdateTask implements Runnable
{
	private final String guid;
	private final double value1;
	private final double value2;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public UpdateTask(String guid, double value1, double value2,
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
		Connection myConn = ThroughputMeasure.dsInst.getConnection();
		Statement statement = null;
		String tableName = "";

		String updateTableSQL = "UPDATE "+
				" SET value1="+value1+", value2="+value2+" where nodeGUID='"+guid+"'";

		try 
		{
			statement = (Statement) myConn.createStatement();
			
			statement.executeUpdate(updateTableSQL);
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