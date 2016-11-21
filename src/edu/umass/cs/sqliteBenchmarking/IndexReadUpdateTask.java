package edu.umass.cs.sqliteBenchmarking;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class IndexReadUpdateTask implements Runnable
{
	private final String searchQuery;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public IndexReadUpdateTask( String searchQuery,
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
			int replySize = executeQuery();
			long end = System.currentTimeMillis();
			requestSendingTask.incrementSearchNumRecvd(replySize, end-start);
		} catch(Exception ex)
		{
			ex.printStackTrace();
		}
		catch(Error ex)
		{
			ex.printStackTrace();
		}
	}
	
	public int executeQuery()
	{
		Connection myConn = null;
		Statement stmt = null;
		int numResults = 0;
		
		try
		{	
			myConn = SQLiteThroughputBenchmarking.dsInst.getConnection(DataSource.UPDATE_POOL);
			stmt = myConn.createStatement();
			
			ResultSet rs = stmt.executeQuery(searchQuery);
			while( rs.next() )
			{
				numResults++;
				//Retrieve by column name
				//double value  	 = rs.getDouble("value");
//				byte[] nodeGUIDBytes  = rs.getBytes("nodeGUID");
//				String nodeGUIDString = Utils.bytArrayToHex(nodeGUIDBytes);
//				jsoArray.put(nodeGUIDString);
			}
			rs.close();
			
			try
			{
				assert(numResults >= 0);
			} catch(Error er)
			{
				System.out.println("searchQuery" + searchQuery);
				er.printStackTrace();
			}
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
		return numResults;
	}
}