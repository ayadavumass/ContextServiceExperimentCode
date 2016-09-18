package edu.umass.cs.mysqlLocationBechmarking;

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
	// indexing types
	
	public static final int NO_INDEXING							= 1;
	public static final int JOINT_INDEXING						= 2;
	public static final int SINGLE_INDEXING						= 3;

	// request types
	public static final int SINGLE_ATTR_REQ						= 1;
	public static final int DOUBLE_ATTR_REQ						= 2;
	
	
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
	
	public static final String dataTableName 					= "testTable";
	
	public static final String triggerTableName 				= "triggerTable";
	
	
	public static final double LAT_MIN							= 42.0;
	public static final double LAT_MAX							= 44.0;
	
	public static final double LONG_MIN							= -80.0;
	public static final double LONG_MAX							= -78.0;
	
	
	public static final int ATTR_DEFAULT						= 0;
	
	public static final int numAttrsInQuery						= 4;
	
	public static DataSource dsInst;
	//private Random valueRand;
	
	public static int nodeId;
	
	public static int numGuids;
	public static int numAttrs;
	
	public static int indexingType;
	public static int requestType;
	public static double requestsps;
	
	public static int numGuidsToInsert;
	
	public static int PoolSize;
	
	public static double predicateLength;
	
	public static long numOfSearchQueries;
	
	public static boolean disableCircularQueryTrigger			= true;
	
	public static ExecutorService	 taskES						= null;
	
	public static boolean rowByrowFetching						= false;
	public static boolean getOnlyCount							= false;
	
	public MySQLThroughputBenchmarking()
	{
		try
		{
			taskES = Executors.newFixedThreadPool(PoolSize);
			//valueRand = new Random();
			dsInst = new DataSource(nodeId);
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
		Connection myConn 	= null;
		Statement stmt 		= null;
		
		try
		{
			myConn = dsInst.getConnection();
			stmt = myConn.createStatement();
			
			String newTableCommand = "drop table "+dataTableName;
			
			try
			{
				stmt.executeUpdate(newTableCommand);
			}
			catch(Exception ex)
			{
				System.out.println("Table delete exception");
			}
			
			switch(indexingType)
			{
				case NO_INDEXING:
				{
					createNoIndexTable(stmt);
					break;
				}
				case JOINT_INDEXING:
				{
					createJointIndexTable(stmt);
					break;
				}
				case SINGLE_INDEXING:
				{
					createSingleIndexTable(stmt);
					break;
				}
			}
			
		}
		catch ( SQLException e )
		{
			e.printStackTrace();
			
			assert(false);
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
	
	
	private void createNoIndexTable(Statement stmt) throws SQLException
	{
		String newTableCommand 
			= "create table "+dataTableName+" ( nodeGUID Binary(20) PRIMARY KEY ";

		
		String attrName = "latitude";
		
		newTableCommand = newTableCommand +" , "+ attrName+" DOUBLE NOT NULL ";
		
		
		attrName = "longitude";
		
		
		newTableCommand = newTableCommand +" , "+ attrName+" DOUBLE NOT NULL ";
		
					
		newTableCommand = newTableCommand +" ) ";
		
		stmt.executeUpdate(newTableCommand);
	}
	
	
	private void createJointIndexTable(Statement stmt) throws SQLException
	{
		String newTableCommand 
			= "create table "+dataTableName+" ( nodeGUID Binary(20) PRIMARY KEY ";
		
		String attrName = "latitude";
		
		newTableCommand = newTableCommand +" , "+ attrName+" DOUBLE NOT NULL ";
		
		
		attrName = "longitude";
		
		newTableCommand = newTableCommand +" , "+ attrName+" DOUBLE NOT NULL ";
		
		
		newTableCommand = newTableCommand +" , INDEX USING BTREE ( latitude , longitude ) ";
		
		
		newTableCommand = newTableCommand +" ) ";
		
		stmt.executeUpdate(newTableCommand);
	}
	
	
	private void createSingleIndexTable(Statement stmt) throws SQLException
	{
		String newTableCommand 
			= "create table "+dataTableName+" ( nodeGUID Binary(20) PRIMARY KEY ";
		
		
		String attrName = "latitude";
	
		newTableCommand = newTableCommand +" , "+ attrName+" DOUBLE NOT NULL ";
	
	
		attrName = "longitude";
	
		newTableCommand = newTableCommand +" , "+ attrName+" DOUBLE NOT NULL ";
	
	
		newTableCommand = newTableCommand +" , INDEX USING BTREE ( latitude ) ";
		newTableCommand = newTableCommand +" , INDEX USING BTREE ( longitude ) ";
	
	
		newTableCommand = newTableCommand +" ) ";
		
		stmt.executeUpdate(newTableCommand);
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
	
	/*private static String getPartitionInfoStorageString(String newTableCommand)
	{
		// query and default value mechanics
		//Attr specified in query but not set in GUID                  Do Not return GUID
		//Attr specified in query and  set in GUID                     Return GUID if possible
		
		//Attr not specified in query but  set in GUID                 Return GUID if possible 
		//Attr not specified in query and not set in GUID              Return GUID if possible as no privacy leak
		
		// creating for all attributes rather 
		// than just the attributes of the subspace for better mataching
		for( int i=0; i<numAttrs; i++ )
		{
			String attrName = "attr"+i;
			String lowerAttrName = "lower"+attrName;
			String upperAttrName = "upper"+attrName;
			
			String queryMinDefault = ATTR_DEFAULT+"";
			String queryMaxDefault = ATTR_MAX+"";
			
			// changed it to min max for lower and upper value instead of default 
			// because we want a query to match for attributes that are not specified 
			// in the query, as those basically are don't care.
			newTableCommand = newTableCommand + " , "+lowerAttrName+" DOUBLE "
					+ " DEFAULT "+ queryMinDefault 
					+ " , "+upperAttrName+" DOUBLE DEFAULT "
					+ queryMaxDefault 
					+ " , INDEX USING BTREE("+lowerAttrName+" , "+upperAttrName+")"
					+ " , INDEX USING HASH("+lowerAttrName+")";
		}
		newTableCommand = newTableCommand +" , INDEX USING BTREE( ";
//		for(int i=0; i<numAttrs; i++)
//		{
//			if(i >= 8)
//				break;
//			String attrName = "attr"+i;
//			String lowerAttrName = "lower"+attrName;
//			String upperAttrName = "upper"+attrName;
//			
//			if(i == 0)
//			{
//				newTableCommand = newTableCommand+ lowerAttrName+" , "+upperAttrName;
//			}
//			else
//			{
//				newTableCommand = newTableCommand+ " , "+lowerAttrName+" , "+upperAttrName;
//			}
//			
//		}
//		newTableCommand = newTableCommand+ " ) ";
		
		return newTableCommand;
	}*/
	
	public static void main( String[] args )
	{
		nodeId 			 	= Integer.parseInt(args[0]);
		numGuids 		 	= Integer.parseInt(args[1]);
		indexingType		= Integer.parseInt(args[2]);
		requestType         = Integer.parseInt(args[3]);
		requestsps          = Integer.parseInt(args[4]);
		PoolSize  		    = Integer.parseInt(args[5]);
		predicateLength     = Double.parseDouble(args[6]);
		rowByrowFetching	= Boolean.parseBoolean(args[7]);
		getOnlyCount        = Boolean.parseBoolean(args[8]);
		
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
		} 
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		
		System.out.println("Reading only count "+getOnlyCount+" rowByrowFetching "+rowByrowFetching);
		AbstractRequestSendingClass requestTypeObj = new SearchClass();
		
		new Thread(requestTypeObj).start();
		requestTypeObj.waitForThreadFinish();
		double avgReplySize = ((SearchClass)requestTypeObj).getAvgResultSize();
		double avgReplyTime = ((SearchClass)requestTypeObj).getAvgTime();
		double searchCapacity = ((SearchClass)requestTypeObj).getSearchCapacity();
		System.out.println("Average result size "
		 +avgReplySize + " avg time "+avgReplyTime+" searchCapacity "+searchCapacity);
		
		System.exit(0);
		//stateChange.waitForThreadFinish();
	}
}