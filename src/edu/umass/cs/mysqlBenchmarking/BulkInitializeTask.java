package edu.umass.cs.mysqlBenchmarking;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;


public class BulkInitializeTask implements Runnable
{
	private final List<UpdateInfoClass> updateList;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public BulkInitializeTask( List<UpdateInfoClass> updateList,
			AbstractRequestSendingClass requestSendingTask )
	{
		this.updateList = updateList;
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
			requestSendingTask.incrementUpdateNumRecvd("", end-start);
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
			myConn = MySQLThroughputBenchmarking.dsInst.getConnection();
			
			String insertTableSQL = "INSERT INTO "
					+ MySQLThroughputBenchmarking.dataTableName 
					+ " ( nodeGUID ";
			
			Iterator<String> attrIter = MySQLThroughputBenchmarking.attrMap.keySet().iterator();
			
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				insertTableSQL = insertTableSQL + " , "+attrName;
			}
			insertTableSQL = insertTableSQL + " ) VALUES ";//( X'"+guid+"' ";
			
			for(int i=0; i<updateList.size(); i++)
			{
				UpdateInfoClass updateInfoClass = updateList.get(i);
				String guid = updateInfoClass.getGUID();
				
				if(i == 0)
				{
					insertTableSQL = insertTableSQL + "( X'"+guid+"' ";
				}
				else
				{
					insertTableSQL = insertTableSQL + " , ( X'"+guid+"' ";
				}
				
				attrIter = MySQLThroughputBenchmarking.attrMap.keySet().iterator();
				
				while( attrIter.hasNext() )
				{
					String attrName = attrIter.next();
					String attrVal = updateInfoClass.getAttrValMap().get(attrName);
					insertTableSQL = insertTableSQL + " , "+attrVal;
				}
				insertTableSQL = insertTableSQL + " ) ";
			}
			
			
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