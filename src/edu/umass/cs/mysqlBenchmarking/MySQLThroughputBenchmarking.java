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
	public static final int RUN_UPDATE							= 1;
	public static final int RUN_SEARCH							= 2;
	public static final int RUN_INSERT							= 3;
	public static final int RUN_GET								= 4;
	public static final int RUN_INDEX_READ_UPDATE				= 5;
	public static final int RUN_INDEX_READ_SEARCH				= 6;
	public static final int RUN_DELETE							= 7;
	public static final int RUN_GET_BACK_TO_BACK				= 8;
	public static final int RUN_UPDATE_BACK_TO_BACK				= 9;
	public static final int RUN_TRIGGER_SEARCH					= 10;
	public static final int RUN_TRIGGER_UPDATE					= 11;
	
	
	
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
	
	public static final String dataTableName 					= "testTable";
	
	public static final String triggerTableName 				= "triggerTable";
	
	
	
	public static final int ATTR_MAX							= 1500;
	public static final int ATTR_MIN							= 1;
	public static final int ATTR_DEFAULT						= 0;
	
	public static final int numAttrsInQuery						= 4;
	
	public static DataSource dsInst;
	//private Random valueRand;
	
	public static int nodeId;
	
	public static int numGuids;
	public static int numAttrs;
	
	public static int requestType;
	public static double requestsps;
	
	public static int numGuidsToInsert;
	
	public static int PoolSize;
	
	public static double predicateLength;
	
	public static long numOfSearchQueries;
	
	public static boolean disableCircularQueryTrigger = true;
	
	public static ExecutorService	 taskES						= null;
	
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
			
			// char 45 for GUID because, GUID is 40 char in length, 5 just additional
			newTableCommand = "create table "+dataTableName+" ( nodeGUID Binary(20) PRIMARY KEY ";
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
			
			newTableCommand = "drop table "+triggerTableName;
			
			try
			{
				stmt.executeUpdate(newTableCommand);
			}
			catch(Exception ex)
			{
				System.out.println("Table delete exception");
			}
			
			createTablesForTriggers(stmt);
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
	
	/**
	 * creates one dimensional subspaces and query storage tables for triggers
	 * @throws SQLException 
	 */
	private static void createTablesForTriggers(Statement stmt) throws SQLException
	{
		//String tableName = "subspaceId"+subspaceId+"TriggerDataInfo";
		String newTableCommand = "create table "+triggerTableName+" ( groupGUID BINARY(20) NOT NULL , "
				+ "userIP Binary(4) NOT NULL ,  userPort INTEGER NOT NULL , expiryTime BIGINT NOT NULL ";
		newTableCommand = getPartitionInfoStorageString(newTableCommand);
		
		newTableCommand = newTableCommand 
				+ " , PRIMARY KEY(groupGUID, userIP, userPort), INDEX USING BTREE(expiryTime), "
				+ " INDEX USING HASH(groupGUID) )";
		System.out.println("newTableCommand "+newTableCommand);
		
		stmt.executeUpdate(newTableCommand);
		
	}
	
	private static String getPartitionInfoStorageString(String newTableCommand)
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
		/*newTableCommand = newTableCommand +" , INDEX USING BTREE( ";
		for(int i=0; i<numAttrs; i++)
		{
			if(i >= 8)
				break;
			String attrName = "attr"+i;
			String lowerAttrName = "lower"+attrName;
			String upperAttrName = "upper"+attrName;
			
			if(i == 0)
			{
				newTableCommand = newTableCommand+ lowerAttrName+" , "+upperAttrName;
			}
			else
			{
				newTableCommand = newTableCommand+ " , "+lowerAttrName+" , "+upperAttrName;
			}
			
		}
		newTableCommand = newTableCommand+ " ) ";*/
		
		return newTableCommand;
	}
	
	public static void main( String[] args )
	{
		nodeId 			 = Integer.parseInt(args[0]);
		numGuids 		 = Integer.parseInt(args[1]);
		numAttrs 		 = Integer.parseInt(args[2]);
		
		requestType        = Integer.parseInt(args[3]);
		requestsps         = Integer.parseInt(args[4]);
		PoolSize  		   = Integer.parseInt(args[5]);
		predicateLength    = Double.parseDouble(args[6]);
		
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
		
//		public static final int RUN_UPDATE							= 1;
//		public static final int RUN_SEARCH							= 2;
//		public static final int RUN_INSERT							= 3;
//		public static final int RUN_GET								= 4;
//		public static final int RUN_INDEX_READ_UPDATE				= 5;
//		public static final int RUN_INDEX_READ_SEARCH				= 6;
//		public static final int RUN_DELETE							= 7;
		
		
		AbstractRequestSendingClass requestTypeObj = null;
		switch( requestType )
		{
			case RUN_UPDATE:
			{
				requestTypeObj = new UpdateClass();
				new Thread(requestTypeObj).start();
				requestTypeObj.waitForThreadFinish();
				double avgUpdTime = ((UpdateClass)requestTypeObj).getAvgUpdateTime();
				System.out.println("Avg update time "+avgUpdTime);
				break;
			}
			case RUN_SEARCH:
			{
				requestTypeObj = new SearchClass();
				new Thread(requestTypeObj).start();
				requestTypeObj.waitForThreadFinish();
				double avgReplySize = ((SearchClass)requestTypeObj).getAvgResultSize();
				double avgReplyTime = ((SearchClass)requestTypeObj).getAvgTime();
				System.out.println("Average result size "
				 +avgReplySize + " avg time "+avgReplyTime);
				break;
			}
			case RUN_INSERT:
			{
				numGuidsToInsert = Integer.parseInt(args[7]);
				requestTypeObj = new InsertClass();
				new Thread(requestTypeObj).start();
				requestTypeObj.waitForThreadFinish();
				break;
			}
			case RUN_GET:
			{
				requestTypeObj = new GetClass();
				new Thread(requestTypeObj).start();
				requestTypeObj.waitForThreadFinish();
				break;
			}
			case RUN_INDEX_READ_UPDATE:
			{
				requestTypeObj = new IndexReadUpdateClass();
				new Thread(requestTypeObj).start();
				requestTypeObj.waitForThreadFinish();
				break;
			}
			case RUN_INDEX_READ_SEARCH:
			{
				requestTypeObj = new IndexReadSearchClass();
				new Thread(requestTypeObj).start();
				requestTypeObj.waitForThreadFinish();
				System.out.println("Average result size "
						 +(((IndexReadSearchClass)requestTypeObj).getAvgResultSize())
						 + " avg attr match "+((IndexReadSearchClass)requestTypeObj).getAvgAttrMatch() );
				break;
			}
			case RUN_DELETE:
			{
				requestTypeObj = new DeleteClass();
				new Thread(requestTypeObj).start();
				requestTypeObj.waitForThreadFinish();
				break;
			}
			case RUN_GET_BACK_TO_BACK:
			{
				requestTypeObj = new GetClassBackToBack();
				new Thread(requestTypeObj).start();
				requestTypeObj.waitForThreadFinish();
				break;
			}
			case RUN_UPDATE_BACK_TO_BACK:
			{
				requestTypeObj = new UpdateClassBackToBack();
				new Thread(requestTypeObj).start();
				requestTypeObj.waitForThreadFinish();
				break;
			}
			case RUN_TRIGGER_SEARCH:
			{
				numOfSearchQueries = Long.parseLong(args[7]);
				requestTypeObj = new TriggerSearchClass();
				new Thread(requestTypeObj).start();
				requestTypeObj.waitForThreadFinish();
				
				System.out.println("Average time "
						 +(((TriggerSearchClass)requestTypeObj).getAvgTime()) );
				
				break;
			}
			case RUN_TRIGGER_UPDATE:
			{
				numOfSearchQueries = Long.parseLong(args[7]);
				requestTypeObj = new TriggerSearchClass();
				new Thread(requestTypeObj).start();
				requestTypeObj.waitForThreadFinish();
				
				System.out.println("Average time "
						 +(((TriggerSearchClass)requestTypeObj).getAvgTime()) );
				
				requestTypeObj = new TriggerUpdateClass();
				new Thread(requestTypeObj).start();
				requestTypeObj.waitForThreadFinish();
				
				System.out.println("Update average time "
						 + (((TriggerUpdateClass)requestTypeObj).getAvgUpdateTime())
						 + " avg removed "
						 + (((TriggerUpdateClass)requestTypeObj).getAvgRemoved())
						 + " avg added "
						 + (((TriggerUpdateClass)requestTypeObj).getAvgAdded()) );
				break;
			}
			default:
				assert(false);
		}
		
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