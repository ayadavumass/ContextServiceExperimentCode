package edu.umass.cs.mysqlBenchmarking;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.utils.Utils;

public class TriggerUpdateTask implements Runnable
{
	private final String removedGroupQuery;
	private final String addedGroupQuery;
	private final JSONArray removedGroups;
	private final JSONArray addedGroups;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public TriggerUpdateTask( String removedGroupQuery, String addedGroupQuery, 
			 JSONArray removedGroups, JSONArray addedGroups, 
			AbstractRequestSendingClass requestSendingTask )
	{
		this.removedGroupQuery = removedGroupQuery;
		this.addedGroupQuery = addedGroupQuery;
		this.removedGroups = removedGroups;
		this.addedGroups = addedGroups;
		
		this.requestSendingTask = requestSendingTask;
	}
	
	@Override
	public void run()
	{
		try 
		{
			long start = System.currentTimeMillis();
			execureQueries();
			long end = System.currentTimeMillis();
			String str = removedGroups.length()+"-"+addedGroups.length();
			requestSendingTask.incrementUpdateNumRecvd(str, end-start);
		} catch (SQLException e) 
		{
			e.printStackTrace();
		}
		
	}
	
	public void execureQueries() throws SQLException
	{
		Connection myConn = null;
		Statement stmt = null;
		
		try 
		{
			myConn = MySQLThroughputBenchmarking.dsInst.getConnection();
			stmt = (Statement) myConn.createStatement();
			
			ResultSet rs = stmt.executeQuery(removedGroupQuery);
			
			while( rs.next() )
			{
				// FIXME: need to replace these with macros
				byte[] groupGUIDBytes = rs.getBytes("groupGUID");
				String groupGUIDString = Utils.bytArrayToHex(groupGUIDBytes);
				byte[] ipAddressBytes = rs.getBytes("userIP");
				String userIPString = InetAddress.getByAddress(ipAddressBytes).getHostAddress();
				int userPort = rs.getInt("userPort");
				
				JSONObject grpJSON = new JSONObject();
				grpJSON.put("groupGUIDString", groupGUIDString);
				grpJSON.put("userIPString", userIPString);
				grpJSON.put("userPort", userPort);
				
				removedGroups.put(grpJSON);
			}
			rs.close();
			
			rs = stmt.executeQuery(addedGroupQuery);
			
			while( rs.next() )
			{
				// FIXME: need to replace these with macros
				byte[] groupGUIDBytes = rs.getBytes("groupGUID");
				String groupGUIDString = Utils.bytArrayToHex(groupGUIDBytes);
				byte[] ipAddressBytes = rs.getBytes("userIP");
				String userIPString = InetAddress.getByAddress(ipAddressBytes).getHostAddress();
				int userPort = rs.getInt("userPort");
				
				JSONObject grpJSON = new JSONObject();
				grpJSON.put("groupGUIDString", groupGUIDString);
				grpJSON.put("userIPString", userIPString);
				grpJSON.put("userPort", userPort);
				
				addedGroups.put(grpJSON);
			}
			rs.close();
			
		} catch (SQLException e) 
		{
			e.printStackTrace();
		} catch (UnknownHostException e) 
		{
			e.printStackTrace();
		} catch (JSONException e) 
		{
			e.printStackTrace();
		} finally
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
	}
}