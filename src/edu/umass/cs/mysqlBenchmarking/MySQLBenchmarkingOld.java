package edu.umass.cs.mysqlBenchmarking;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MySQLBenchmarkingOld
{
	// 100 seconds, experiment runs for 100 seconds
	public static final int EXPERIMENT_TIME						= 100000;
	
	// after sending all the requests it waits for 100 seconds 
	public static final int WAIT_TIME							= 100000;
	
	// 1% loss tolerance
	public static final double INSERT_LOSS_TOLERANCE			= 0.5;
			
	// 1% loss tolerance
	public static final double UPD_LOSS_TOLERANCE				= 0.5;
		
	// 1% loss tolerance
	public static final double SEARCH_LOSS_TOLERANCE			= 0.5;
	
	public static String guidPrefix								= "guidPrefix";
	
	public static final String tableName 						= "testTable";
	
	
	public static DataSource dsInst;
	//private Random valueRand;
	public static double searchRequestsps;
	public static double updateRequestsps;
	public static int numGuids;
	
	public static boolean runUpdate ;
	public static boolean runSearch ;
	
	
	public static ExecutorService	 taskES												= null;
	
	public MySQLBenchmarkingOld()
	{
		Connection myConn = null;
		Statement stmt = null;
		
		try
		{
			taskES = Executors.newFixedThreadPool(100);
			
			//valueRand = new Random();
			dsInst = new DataSource();
			
			myConn = dsInst.getConnection();
			
			stmt = myConn.createStatement();
			// char 45 for GUID because, GUID is 40 char in length, 5 just additional
			String newTableCommand = "create table "+MySQLBenchmarkingOld.tableName+" ( "
					+ "   value1 DOUBLE NOT NULL, value2 DOUBLE NOT NULL, nodeGUID CHAR(100) PRIMARY KEY, versionNum INT NOT NULL,"
					+ " INDEX USING BTREE (value1), INDEX USING BTREE (value2) )";
			
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
	
	public static void main(String[] args)
	{
		numGuids = Integer.parseInt(args[0]);
		searchRequestsps = Double.parseDouble(args[1]);
		updateRequestsps = Double.parseDouble(args[2]);
		runUpdate = Boolean.parseBoolean(args[3]);
		runSearch = Boolean.parseBoolean(args[4]);
		MySQLBenchmarkingOld mysqlBech = new MySQLBenchmarkingOld();
		
		long start = System.currentTimeMillis();
		InitializeClass initClass = new InitializeClass();
		new Thread(initClass).start();
		initClass.waitForThreadFinish();
//		mysqlBech.insertRecords();
		System.out.println(numGuids+" records inserted in "+(System.currentTimeMillis()-start));
		
		try
		{
			Thread.sleep(10000);
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		UpdateClass updateObj = null;
		SearchClass searchObj = null;
		
		if(runUpdate)
			updateObj = new UpdateClass();
		if(runSearch)
			searchObj = new SearchClass();
		

		if(runUpdate)
			new Thread(updateObj).start();
		if(runSearch)
			new Thread(searchObj).start();
		
		if(runSearch)
			searchObj.waitForThreadFinish();
		if(runUpdate)
			updateObj.waitForThreadFinish();
		
		System.exit(0);
		//stateChange.waitForThreadFinish();
	}
	
	/*public void insertRecords()
	{
		//String guidName = "guid";
		for(int i=0;i<numGuids;i++)
		{
			String guid = getSHA1(guidPrefix+i);
			try 
			{
				putValueObjectRecord(1+valueRand.nextInt(1499), 1+valueRand.nextInt(1499), guid, -1);
			} catch (SQLException e) 
			{
				e.printStackTrace();
			}
			System.out.println(i+" records inserted");
		}
	}*/
	
	/*public void putValueObjectRecord(double value1, double value2, String nodeGUID, 
			long versionNum) throws SQLException
	{
		Connection myConn = dsInst.getConnection();
		Statement statement = null;

		String tableName = "testTable";
		String insertTableSQL = "INSERT INTO "+tableName 
				+" (value1, value2, nodeGUID, versionNum) " + "VALUES"
				+ "("+value1+","+value2+",'"+nodeGUID+"',"+versionNum +")";

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
	}*/
}