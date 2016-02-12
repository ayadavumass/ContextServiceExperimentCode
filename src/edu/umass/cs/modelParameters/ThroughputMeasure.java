package edu.umass.cs.modelParameters;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONArray;


public class ThroughputMeasure
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
	
	public static final String tableName 						= "subspaceId0DataStorage";
	
	public static final double LONGITUDE_MIN 					= -98.08;
	public static final double LONGITUDE_MAX 					= -96.01;
	
	public static final double LATITUDE_MAX 					= 33.635;
	public static final double LATITUDE_MIN 					= 31.854;
	
	
	public static DataSource dsInst;
	//private Random valueRand;
	public static double searchRequestsps;
	public static double updateRequestsps;
	public static int numGuids;
	
	public static boolean runUpdate ;
	public static boolean runSearch ;
	
	
	public static ExecutorService	 taskES						= null;
	
	public static final String latitudeAttrName					= "geoLocationCurrentLat";
	public static final String longitudeAttrName				= "geoLocationCurrentLong";
	
	public ThroughputMeasure()
	{	
		try
		{
			taskES = Executors.newFixedThreadPool(100);
			
			dsInst = new DataSource();
			testTableSize();
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
	
	public static void testTableSize()
	{
		System.out.println("testTableSize called");
		Connection myConn = null;
		Statement stmt 	  = null;
		
		String selectTestQuery = "select count(*) as size from subspaceId0DataStorage";
		try
		{
			myConn = ThroughputMeasure.dsInst.getConnection();
			stmt = myConn.createStatement();
				
			ResultSet rs = stmt.executeQuery(selectTestQuery);
				
			while( rs.next() )
			{
				String tableSize = rs.getString("size");
				System.out.println("tableSize "+tableSize);
				//resultSize++;
				//jsoArray.put(nodeGUID);
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
		searchRequestsps = Double.parseDouble(args[0]);
		updateRequestsps = Double.parseDouble(args[1]);
		runUpdate = Boolean.parseBoolean(args[2]);
		runSearch = Boolean.parseBoolean(args[3]);
		ThroughputMeasure throughputBech = new ThroughputMeasure();
		
//		long start = System.currentTimeMillis();
//		InitializeClass initClass = new InitializeClass();
//		new Thread(initClass).start();
//		initClass.waitForThreadFinish();
////			mysqlBech.insertRecords();
//		System.out.println(numGuids+" records inserted in "+(System.currentTimeMillis()-start));
		
//		try
//		{
//			Thread.sleep(10000);
//		} catch (InterruptedException e)
//		{
//			e.printStackTrace();
//		}
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
}