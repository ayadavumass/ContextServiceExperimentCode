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

public class MySQLThroughputBenchmarking
{
	public static final int NUM_SUBSPACES						= 4;
	
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
	
	public static final String guidPrefix						= "guidPrefix";
	
	public static final String tableName 						= "testTable";
	
	public static final int ATTR_MAX							= 1500;
	public static final int ATTR_MIN							= 1;
	
	public static final int numAttrsInQuery						= 4;
	
	public static DataSource dsInst;
	//private Random valueRand;
	public static double searchRequestsps;
	public static double updateRequestsps;
	public static double insertRequestsps;
	public static double getRequestsps;
	public static double indexReadRequestsps;
	
	
	public static int numGuids;
	public static int numAttrs;
	
	public static boolean runUpdate;
	public static boolean runSearch;
	public static boolean runInsert;
	public static boolean runGet;
	public static boolean runIndexRead; 
	
	public static int PoolSize;
	
	public static ExecutorService	 taskES						= null;
	
	public MySQLThroughputBenchmarking()
	{
		try
		{
			taskES = Executors.newFixedThreadPool(PoolSize);
			//valueRand = new Random();
			dsInst = new DataSource();
			createTable();
			
//			myConn = dsInst.getConnection();
//			stmt = myConn.createStatement();
//			// char 45 for GUID because, GUID is 40 char in length, 5 just additional
//			String newTableCommand = "create table "+tableName+" ( "
//					+ "   value1 DOUBLE NOT NULL, value2 DOUBLE NOT NULL, nodeGUID CHAR(100) PRIMARY KEY, versionNum INT NOT NULL,"
//					+ " INDEX USING BTREE (value1), INDEX USING BTREE (value2) )";
//			stmt.executeUpdate(newTableCommand);
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (SQLException e)
		{
			e.printStackTrace();
		} catch (PropertyVetoException e)
		{
			e.printStackTrace();
		}
	}
	
	private void createTable()
	{
		Connection myConn = null;
		Statement stmt = null;
		
		try
		{	
			myConn = dsInst.getConnection();
			stmt = myConn.createStatement();
			
			String newTableCommand = "delete table "+tableName;
			stmt.executeUpdate(newTableCommand);
			
			// char 45 for GUID because, GUID is 40 char in length, 5 just additional
			newTableCommand = "create table "+tableName+" ( nodeGUID Binary(20) PRIMARY KEY ";
					//+ "   value1 DOUBLE NOT NULL, value2 DOUBLE NOT NULL, nodeGUID CHAR(100) PRIMARY KEY, versionNum INT NOT NULL,"
					//+ " INDEX USING BTREE (value1), INDEX USING BTREE (value2) )";
			
			for( int i=0; i<numAttrs; i++ )
			{
				String attrName = "attr"+i;
				
				newTableCommand = newTableCommand +" , "+ attrName+" DOUBLE NOT NULL , "
						+ "INDEX USING BTREE ("+attrName+")";
			}
			
			newTableCommand = newTableCommand +" ) ";
			
//			String newTableCommand = "create table "+tableName+" ( "
//					+ "   value1 DOUBLE NOT NULL, value2 DOUBLE NOT NULL, nodeGUID CHAR(100) PRIMARY KEY, versionNum INT NOT NULL,"
//					+ " INDEX USING BTREE (value1), INDEX USING BTREE (value2) )";
			stmt.executeUpdate(newTableCommand);
		} 
		catch ( SQLException e )
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				if(stmt != null)
					stmt.close();
				
				if(myConn != null)
					myConn.close();
			}
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	public static String getSHA1(String stringToHash)
	{
		MessageDigest md = null;
		try
		{
			md = MessageDigest.getInstance("SHA-256");
		} 
		catch (NoSuchAlgorithmException e)
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
	
	public static void main( String[] args )
	{
		numGuids = Integer.parseInt(args[0]);
		numAttrs = Integer.parseInt(args[1]);
		updateRequestsps = Double.parseDouble(args[2]);
		searchRequestsps = Double.parseDouble(args[3]);
		insertRequestsps = Double.parseDouble(args[4]);
		getRequestsps 	 = Double.parseDouble(args[5]);
		indexReadRequestsps = Double.parseDouble(args[6]);
		
		runUpdate = Boolean.parseBoolean(args[7]);
		runSearch = Boolean.parseBoolean(args[8]);
		runInsert = Boolean.parseBoolean(args[9]);
		runGet    = Boolean.parseBoolean(args[10]);
		runIndexRead = Boolean.parseBoolean(args[11]);
		PoolSize  = Integer.parseInt(args[12]);
		
		
		MySQLThroughputBenchmarking mysqlBech 
								= new MySQLThroughputBenchmarking();
		
		long start = System.currentTimeMillis();
		InitializeClass initClass = new InitializeClass();
		new Thread(initClass).start();
		initClass.waitForThreadFinish();
//			mysqlBech.insertRecords();
		System.out.println(numGuids+" records inserted in "
									+(System.currentTimeMillis()-start));
		
		try
		{
			Thread.sleep(10000);
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		
		
		UpdateClass updateObj 				= null;
		SearchClass searchObj 				= null;
		InsertClass insertObj 				= null;
		GetClass getObj 	  				= null;
		IndexReadSearchClass indexSearchObj = null;
		
		if(runUpdate)
			updateObj = new UpdateClass();
		if(runSearch)
			searchObj = new SearchClass();
		if(runInsert)
			insertObj = new InsertClass();
		if(runGet)
			getObj    = new GetClass();
		if(runIndexRead)
			indexSearchObj = new IndexReadSearchClass();
		
		
		if(runUpdate)
			new Thread(updateObj).start();
		if(runSearch)
			new Thread(searchObj).start();
		if(runInsert)
			new Thread(insertObj).start();
		if(runGet)
			new Thread(getObj).start();
		if(runIndexRead)
			new Thread(indexSearchObj).start();
		
		
		if(runUpdate)
			updateObj.waitForThreadFinish();
		if(runSearch)
			searchObj.waitForThreadFinish();
		if(runInsert)
			insertObj.waitForThreadFinish();
		if(runGet)
			getObj.waitForThreadFinish();
		if(runIndexRead)
			indexSearchObj.waitForThreadFinish();
		
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