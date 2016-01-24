package edu.umass.cs.expcode;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONArray;


public class MySQLBenchmarking
{
	// 100 seconds, experiment runs for 100 seconds
	public static final int EXPERIMENT_TIME										= 100000;
	
	private DataSource dsInst;
	private Random valueRand;
	private static double requestsps;
	private static int numGuids;
	
	private long expStartTime;
	
	private static double currNumReqSent 										= 0;
	private static double currNumReqComp 										= 0;
	
	
	private Random queryRand;
	
	public ExecutorService	 eservice											= null;
	private final Object numComplMonitor										= new Object();
	private boolean sendingFinished												= false;
	
	
	public MySQLBenchmarking()
	{
		Connection myConn = null;
		Statement stmt = null;
		
		try
		{
			eservice = Executors.newFixedThreadPool(5);
			
			valueRand = new Random();
			queryRand = new Random();
			dsInst = new DataSource();
			
			myConn = dsInst.getConnection();
			
			stmt = myConn.createStatement();
			String tableName = "testTable";
			// char 45 for GUID because, GUID is 40 char in length, 5 just additional
			String newTableCommand = "create table "+tableName+" ( "
					+ "   value DOUBLE NOT NULL, nodeGUID CHAR(100) PRIMARY KEY, versionNum INT NOT NULL,"
					+ " INDEX USING BTREE (value) )";
			
			stmt.executeUpdate(newTableCommand);
			
		} catch (IOException e) 
		{
			e.printStackTrace();
		} catch (SQLException e)
		{
			e.printStackTrace();
		} catch (PropertyVetoException e)
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
	
	public void insertRecords()
	{
		String guidName = "guid";
		for(int i=0;i<numGuids;i++)
		{
			String guid = getSHA1(guidName+i);
			try 
			{
				putValueObjectRecord(1+valueRand.nextInt(1499), guid, -1);
			} catch (SQLException e) 
			{
				e.printStackTrace();
			}
			ContextServiceLogger.getLogger().fine(i+" records inserted");
		}
	}
	
	public static String getSHA1(String stringToHash)
	{
	   MessageDigest md=null;
	   try
	   {
		   md = MessageDigest.getInstance("SHA-256");
	   } catch (NoSuchAlgorithmException e)
	   {
		   e.printStackTrace();
	   }
       
	   md.update(stringToHash.getBytes());
	   
       byte byteData[] = md.digest();
 
       //convert the byte to hex format method 1
       StringBuffer sb = new StringBuffer();
       for (int i = 0; i < byteData.length; i++) 
       {
       		sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
       }
       String returnGUID = sb.toString();
       return returnGUID.substring(0, 40);
	}
	
	
	public void putValueObjectRecord(double value, String nodeGUID, 
			long versionNum) throws SQLException
	{
		Connection myConn = dsInst.getConnection();
		Statement statement = null;

		String tableName = "testTable";
		String insertTableSQL = "INSERT INTO "+tableName 
				+" (value, nodeGUID, versionNum) " + "VALUES"
				+ "("+value+",'"+nodeGUID+"',"+versionNum +")";

		try 
		{
			statement = (Statement) myConn.createStatement();

			// execute insert SQL stetement
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
	
	
	public void sendSearch() throws Exception
	{
		long currTime = 0;
		expStartTime = System.currentTimeMillis();
		double numberShouldBeSentPerSleep = requestsps/10.0;
		
		
		while( ( (System.currentTimeMillis() - expStartTime) < EXPERIMENT_TIME ) )
		{
			for(int i=0;i<numberShouldBeSentPerSleep;i++)
			{
				int beg = this.queryRand.nextInt(1400);
		    	int end = beg+this.queryRand.nextInt(1500 - beg-3);
		    	ExecuteSearchThread est = new ExecuteSearchThread(beg, end, System.currentTimeMillis());
		    	currNumReqSent++;
		    	eservice.execute(est);
			}
			currTime = System.currentTimeMillis();
			
			double timeElapsed = ((currTime- expStartTime)*1.0)/1000.0;
			double numberShouldBeSentByNow = timeElapsed*requestsps;
			double needsToBeSentBeforeSleep = numberShouldBeSentByNow - currNumReqSent;
			if(needsToBeSentBeforeSleep > 0)
			{
				needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
			}
			
			for(int i=0;i<needsToBeSentBeforeSleep;i++)
			{
				int beg = this.queryRand.nextInt(1400);
		    	int end = beg+this.queryRand.nextInt(1500 - beg-3);
				ExecuteSearchThread est = new ExecuteSearchThread(beg, end, System.currentTimeMillis());
				currNumReqSent++;
				eservice.execute(est);
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (currNumReqSent * 1.0)/(timeInSec);
		ContextServiceLogger.getLogger().fine("Eventual sending rate "+sendingRate+" currNumReqSent "+currNumReqSent);
		
		sendingFinished = true;
		if(sendingFinished && (currNumReqComp >= currNumReqSent))
		{
			ContextServiceLogger.getLogger().fine("All reqs compl");
			System.exit(0);
		}

		Thread.sleep(100000);
		//double endTimeReplyRecvd = System.currentTimeMillis();
		//double sysThrput= (currNumReplyRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		//ContextServiceLogger.getLogger().fine("Result:Throughput "+sysThrput);
		System.exit(0);
	}
	
	private class ExecuteSearchThread implements Runnable
	{
		private double queryMin;
		private double queryMax;
		private long startTime;
		
		public ExecuteSearchThread(double queryMin, double queryMax, long startTime)
		{
			this.queryMin = queryMin;
			this.queryMax = queryMax;
			this.startTime = startTime;
		}
		
		public JSONArray getValueInfoObjectRecord
		(double queryMin, double queryMax)
		{
			JSONArray jsoArray = new JSONArray();
			Connection myConn = null;
			Statement stmt = null;
			
			try
			{
				String tableName = "testTable";
				
				String selectTableSQL = "SELECT value, nodeGUID from "+tableName+" WHERE "
				+ "( value >= "+queryMin +" AND value < "+queryMax+" )";
				
				myConn = dsInst.getConnection();
				stmt = myConn.createStatement();
				
				ResultSet rs = stmt.executeQuery(selectTableSQL);
				
				while( rs.next() )
				{
					//Retrieve by column name
					//double value  	 = rs.getDouble("value");
					String nodeGUID = rs.getString("nodeGUID");
				
					jsoArray.put(nodeGUID);
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
			return jsoArray;
		}
		
		@Override
		public void run() 
		{
			JSONArray resultArr = getValueInfoObjectRecord(queryMin, queryMax);
			ContextServiceLogger.getLogger().fine("Time taken "+(System.currentTimeMillis()-startTime)+ " result size "+resultArr.length());
			synchronized(numComplMonitor)
			{
				currNumReqComp++;
				if(sendingFinished && (currNumReqComp >= currNumReqSent))
				{
					ContextServiceLogger.getLogger().fine("All reqs compl");
					System.exit(0);
				}
			}
		}
	}
	
	
	public static void main(String[] args)
	{
		numGuids = Integer.parseInt(args[0]);
		requestsps = Double.parseDouble(args[1]);
		MySQLBenchmarking mysqlBech = new MySQLBenchmarking();
		
		long start = System.currentTimeMillis();
		mysqlBech.insertRecords();
		ContextServiceLogger.getLogger().fine(numGuids+" records inserted in "+(System.currentTimeMillis()-start));
		
		try 
		{
			Thread.sleep(10000);
		} catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
		
		try 
		{
			mysqlBech.sendSearch();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}