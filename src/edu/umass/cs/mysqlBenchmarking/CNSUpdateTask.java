package edu.umass.cs.mysqlBenchmarking;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;


import org.json.JSONException;
import org.json.JSONObject;

/**
 * Class implements the task used for 
 * update info in GNS, which is blocking so this 
 * class's object is passed in executor service
 * @author adipc
 */
public class CNSUpdateTask implements Runnable
{
	private final String guid;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public CNSUpdateTask(String guid,
			AbstractRequestSendingClass requestSendingTask)
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
			JSONObject json = getGUIDStoredUsingHashIndex( guid );
			
			double newLat = json.getDouble(CNSUpdateClass.LATITUDE_KEY)+CNSUpdateClass.UPDATE_THRESH;
			double newLong = json.getDouble(CNSUpdateClass.LONGITUDE_KEY)+CNSUpdateClass.UPDATE_THRESH;
			
			if(newLat >= CNSUpdateClass.MIN_US_LAT && newLat < CNSUpdateClass.MAX_US_LAT 
					&& newLong >= CNSUpdateClass.MIN_US_LONG && newLong < CNSUpdateClass.MAX_US_LONG )
			{
				JSONObject updatejson = new JSONObject();
				updatejson.put(CNSUpdateClass.LATITUDE_KEY, newLat);
					
					updatejson.put(CNSUpdateClass.LONGITUDE_KEY, newLong);
					
					updateObject(MySQLThroughputBenchmarking.CNS_HASH_INDEX_TABLE, updatejson);
					updateObject(MySQLThroughputBenchmarking.CNS_ATTR_INDEX_TABLE, updatejson);
			}
			else
			{
				System.out.println("Update value not in range");
			}
			long end = System.currentTimeMillis();
			requestSendingTask.incrementUpdateNumRecvd(guid, end-start);
		} 
		catch (SQLException | JSONException e) 
		{
			e.printStackTrace();
		}
	}
	
	
	public JSONObject getGUIDStoredUsingHashIndex( String guid )
	{
		Connection myConn 		= null;
		Statement stmt 			= null;
		
		String selectQuery 		= "SELECT * ";
		String tableName 		= MySQLThroughputBenchmarking.CNS_HASH_INDEX_TABLE;
		
		JSONObject oldValueJSON = new JSONObject();
		
		selectQuery = selectQuery + " FROM "+tableName+" WHERE nodeGUID = X'"+guid+"'";
		
		try
		{
			myConn = MySQLThroughputBenchmarking.dsInst.getConnection();
			stmt = myConn.createStatement();
			ResultSet rs = stmt.executeQuery(selectQuery);
			
			while( rs.next() )
			{
				ResultSetMetaData rsmd = rs.getMetaData();
				
				int columnCount = rsmd.getColumnCount();
				
				// The column count starts from 1
				for (int i = 1; i <= columnCount; i++ ) 
				{
					String colName = rsmd.getColumnName(i);
					String colVal = rs.getString(colName);
					try {
						oldValueJSON.put(colName, colVal);
					} catch (JSONException e) {
						e.printStackTrace();
					}
				}
			}
			rs.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		} finally
		{
			try
			{
				if (stmt != null)
					stmt.close();
				if (myConn != null)
					myConn.close();
			}
			catch(SQLException e)
			{
				e.printStackTrace();
			}
		}
		
		return oldValueJSON;
	}
	
	
	private void updateObject(String tableName, JSONObject updateJSON) throws SQLException
	{
		Connection myConn = null;
		Statement statement = null;

		try 
		{
			Iterator<String> attrIter = updateJSON.keys();
			
			String updateTableSQL = "UPDATE "+ tableName+
					" SET ";
					//+ "value1="+value1+", value2="+value2+" where nodeGUID='"+guid+"'";
			boolean first = true;
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				
				try
				{
					String value = updateJSON.getString(attrName);
					
					if( first )
					{
						updateTableSQL = updateTableSQL + attrName+" = "+value;
						first = false;
					}
					else
					{
						updateTableSQL = updateTableSQL +" , "+attrName+" = "+value;
					}
				}
				catch(JSONException jsoExcp)
				{
					jsoExcp.printStackTrace();
				}
			}
			updateTableSQL = updateTableSQL +" where nodeGUID=X'"+guid+"'";
			
//			String updateTableSQL = "UPDATE "+ MySQLThroughputBenchmarking.tableName+
//					" SET value1="+value1+", value2="+value2+" where nodeGUID='"+guid+"'";

			myConn = MySQLThroughputBenchmarking.dsInst.getConnection();
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