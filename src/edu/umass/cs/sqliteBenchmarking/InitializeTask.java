package edu.umass.cs.sqliteBenchmarking;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import org.json.JSONException;
import org.json.JSONObject;

public class InitializeTask implements Runnable
{
	private final String guid;
	private final JSONObject attrValJSON;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public InitializeTask(String guid, JSONObject attrValJSON,
			AbstractRequestSendingClass requestSendingTask)
	{
		this.guid = guid;
		this.attrValJSON = attrValJSON;
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
		Connection myConn = null;
		Statement statement = null;
		
		try
		{
			myConn = SQLiteThroughputBenchmarking.dsInst.getConnection();
			
//			String insertTableSQL = "INSERT INTO "+MySQLBenchmarking.tableName 
//					+" (value1, value2, nodeGUID, versionNum) " + "VALUES"
//					+ "("+value1+","+value2+",'"+guid+"',"+-1 +")";
			
			String insertTableSQL = "INSERT INTO "
					+ SQLiteThroughputBenchmarking.dataTableName 
					+ " ( nodeGUID ";
			
			Iterator<String> attrIter = attrValJSON.keys();
			
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				insertTableSQL = insertTableSQL + " , "+attrName;
			}
			insertTableSQL = insertTableSQL + " ) VALUES ( X'"+guid+"' ";
			
			
			attrIter = attrValJSON.keys();
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				try 
				{
					String value = attrValJSON.getString(attrName);
					insertTableSQL = insertTableSQL + " , "+value;
				} catch (JSONException e) 
				{
					e.printStackTrace();
				}
			}
			insertTableSQL = insertTableSQL + " ) ";
			
			statement = (Statement) myConn.createStatement();
			statement.executeUpdate(insertTableSQL);
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