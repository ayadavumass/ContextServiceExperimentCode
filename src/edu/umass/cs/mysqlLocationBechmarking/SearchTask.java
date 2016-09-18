package edu.umass.cs.mysqlLocationBechmarking;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.json.JSONArray;

import edu.umass.cs.contextservice.utils.Utils;

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
			long start = System.currentTimeMillis();
			JSONArray resultArray = new JSONArray();
			int resultSize =  getValueInfoObjectRecord(resultArray);
			long end = System.currentTimeMillis();
			requestSendingTask.incrementSearchNumRecvd(resultSize, end-start);
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
		Statement stmt = null;
		int resultSize = 0;
		try
		{	
			myConn = MySQLThroughputBenchmarking.dsInst.getConnection();
			
			if(MySQLThroughputBenchmarking.rowByrowFetching)
			{
				stmt = myConn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
			              java.sql.ResultSet.CONCUR_READ_ONLY);
				stmt.setFetchSize(Integer.MIN_VALUE);
			}
			else
			{
				stmt = myConn.createStatement();
			}
			
			
			ResultSet rs = stmt.executeQuery(searchQuery);
			
			while( rs.next() )
			{
				//Retrieve by column name
				if( MySQLThroughputBenchmarking.getOnlyCount )
				{
					resultSize = rs.getInt("RESULT_SIZE");
				}
				else
				{
					byte[] nodeGUIDBytes  = rs.getBytes("nodeGUID");
					String nodeGUIDString = Utils.bytArrayToHex(nodeGUIDBytes);
					resultSize++;
					resultArray.put(nodeGUIDString);
				}
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