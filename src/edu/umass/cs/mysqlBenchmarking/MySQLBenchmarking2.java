package edu.umass.cs.mysqlBenchmarking;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.Vector;

/**
 * This class benchmarks the the mysql operations
 * that are done in context service.
 * @author adipc
 */
public class MySQLBenchmarking2 
{
	public static DataSource dsInst;
	public static void main(String[] args) 
			throws IOException, SQLException, PropertyVetoException
	{
		dsInst = new DataSource();
		
		// check time for update
		benchmarkAttrUpdateTime();
	}
	
	
	public static void benchmarkAttrUpdateTime()
	{
		// UPDATE subspaceId0DataStorage SET attr1 = 1000.0 WHERE
		//	nodeGUID=X'CCD658B6AD2EA71D28D63C93CF44634B21969463';
		//
		
		// first select all guids and then we just run updates on them 
		// to get the update latency.
		
		String selectTableSQL = "SELECT HEX(nodeGUID) as nodeGUID FROM subspaceId0DataStorage";
		
		Connection myConn = null;
		Statement statement = null;
		try
		{
			myConn = dsInst.getConnection();
			
			statement = (Statement) myConn.createStatement();
			
			ResultSet rs = statement.executeQuery(selectTableSQL);
			
			Vector<String> guidList = new Vector<String>();
			
			while( rs.next() )
			{
				//Retrieve by column name
				//double value  	 = rs.getDouble("value");
				String nodeGUID = rs.getString("nodeGUID");
				guidList.add(nodeGUID);
			}
			
			Vector<Double> resultlist = new Vector<Double>();
			Random rand = new Random();
			for(int i=0; i<100; i++)
			{
				String updateGUID = guidList.get(rand.nextInt(guidList.size()));
				String updateAttr = "attr"+rand.nextInt(6);
				double udpateVal = rand.nextDouble()*1500.0;
				
				String updateTableSQL = "UPDATE subspaceId0DataStorage "+
						" SET "+updateAttr+" = "+udpateVal+" WHERE nodeGUID=X'"+updateGUID+"'";
				
				long start = System.currentTimeMillis();
				statement.executeUpdate(updateTableSQL);
				long end = System.currentTimeMillis();
				
				double timeTaken = end-start;
				resultlist.add(timeTaken);
				
				try 
				{
					Thread.sleep(100);
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
			System.out.println("Update times "+
			StatisticsClass.toString(StatisticsClass.computeStats(resultlist)));
		} catch (SQLException e) 
		{
			e.printStackTrace();
		} finally
		{
			try 
			{
				if( statement != null )
					statement.close();
				
				if( myConn != null )
					myConn.close();
				
			} catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	public static void benchmarkAttrInsertTime()
	{
		//UPDATE subspaceId0DataStorage SET attr1 = 1000.0 WHERE
		//	nodeGUID=X'CCD658B6AD2EA71D28D63C93CF44634B21969463';
		//
//		Connection myConn = MySQLBenchmarking.dsInst.getConnection();
//		Statement statement = null;
//
//		String updateTableSQL = "UPDATE "+ MySQLBenchmarking.tableName+
//				" SET value1="+value1+", value2="+value2+" where nodeGUID='"+guid+"'";
//
//		try 
//		{
//			statement = (Statement) myConn.createStatement();
//			
//			statement.executeUpdate(updateTableSQL);
//		} catch (SQLException e) 
//		{
//			e.printStackTrace();
//		} finally
//		{
//			try 
//			{
//				if(statement != null)
//					statement.close();
//				
//				if(myConn != null)
//					myConn.close();
//				
//			} catch (SQLException e) 
//			{
//				e.printStackTrace();
//			}
//		}
	}
	
}