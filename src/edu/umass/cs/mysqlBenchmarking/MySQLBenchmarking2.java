package edu.umass.cs.mysqlBenchmarking;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.Vector;

import edu.umass.cs.contextservice.utils.Utils;

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
		dsInst = new DataSource(0);
		
		// check time for update
		benchmarkAttrUpdateTime();
		
		try 
		{
			Thread.sleep(5000);
		} catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
		
		benchmarkPrivacyInsertTimeSingleInsert();
		
		try 
		{
			Thread.sleep(5000);
		} catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
		
		
		benchmarkPrivacyInsertTimeMultipleInsert();
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
			System.out.println("benchmarkAttrUpdateTime: Update times "+
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
	
	public static void benchmarkPrivacyInsertTimeSingleInsert()
	{
		//INSERT INTO attr0EncryptionInfoStorage (nodeGUID, realIDEncryption, subspaceId) VALUES 
		//(X'5D8464F1B7FFEF9EAFF42370E56E0F8FF1C9AB64' , X'5AD69BC3F059A5EB14DFB04400DB029E9287E8C46DABAF5EE79C3A9451095A1F4D06FE6326DAEF0C58A78D146FCF63593D397B0BB37D341C97472B9C7FB450B63C3AAD463E79DE5E9E74778AD330566F30445D97E88198893946CB08EFDA0DF8F82E39DFBE0487CE23E4F5252DBF92A5D2A44A467EC48C5653040AD0C61F038C', 1);	
		String sampleInsertSQL 
			= "INSERT INTO attr0EncryptionInfoStorage (nodeGUID, realIDEncryption, subspaceId) VALUES";
		
		Connection myConn = null;
		Statement statement = null;
		try
		{
			myConn = dsInst.getConnection();
			
			statement = (Statement) myConn.createStatement();
			
			
			Vector<Double> resultlist = new Vector<Double>();
			Random rand = new Random();
			for(int i=0; i<100; i++)
			{
				byte[] guidBytes = new byte[20];
				rand.nextBytes(guidBytes);
				String guidString = Utils.byteArrayToHex(guidBytes);
				
				byte[] realIDEncryptionBytes = new byte[128];
				rand.nextBytes(realIDEncryptionBytes);
				String realIDHex = Utils.byteArrayToHex(realIDEncryptionBytes);
				
				int subsapceId = rand.nextInt(3);
				
				String insertSQL = sampleInsertSQL+"(X'"+guidString+"' , X'"+realIDHex
						+"' , "+subsapceId+")";
				
				long start = System.currentTimeMillis();
				statement.executeUpdate(insertSQL);
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
			System.out.println("benchmarkPrivacyInsertTimeSingleInsert: Insert times "+
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
	
	public static void benchmarkPrivacyInsertTimeMultipleInsert()
	{
		//INSERT INTO attr0EncryptionInfoStorage (nodeGUID, realIDEncryption, subspaceId) VALUES 
		//(X'5D8464F1B7FFEF9EAFF42370E56E0F8FF1C9AB64' , X'5AD69BC3F059A5EB14DFB04400DB029E9287E8C46DABAF5EE79C3A9451095A1F4D06FE6326DAEF0C58A78D146FCF63593D397B0BB37D341C97472B9C7FB450B63C3AAD463E79DE5E9E74778AD330566F30445D97E88198893946CB08EFDA0DF8F82E39DFBE0487CE23E4F5252DBF92A5D2A44A467EC48C5653040AD0C61F038C', 1);
		
		String sampleInsertSQL 
			= "INSERT INTO attr0EncryptionInfoStorage (nodeGUID, realIDEncryption, subspaceId) VALUES";
		
		Connection myConn = null;
		Statement statement = null;
		try
		{
			myConn = dsInst.getConnection();
			
			statement = (Statement) myConn.createStatement();
			
			
			Vector<Double> resultlist = new Vector<Double>();
			Random rand = new Random(System.currentTimeMillis());
			for(int i=0; i<100; i++)
			{
				String insertSQL = sampleInsertSQL ;
				
				for(int j=0; j<5; j++)
				{
					if(j != 0)
					{
						insertSQL = insertSQL +" , ";
					}
					
					byte[] guidBytes = new byte[20];
					rand.nextBytes(guidBytes);
					String guidString = Utils.byteArrayToHex(guidBytes);
					
					byte[] realIDEncryptionBytes = new byte[128];
					rand.nextBytes(realIDEncryptionBytes);
					String realIDHex = Utils.byteArrayToHex(realIDEncryptionBytes);
					
					int subsapceId = rand.nextInt(3);
					
					insertSQL = insertSQL+ "(X'"+guidString+"' , X'"+realIDHex
							+"' , "+subsapceId+")";
				}
				
				long start = System.currentTimeMillis();
				statement.executeUpdate(insertSQL);
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
			System.out.println("benchmarkPrivacyInsertTimeMultipleInsert: Insert times "+
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
}