package edu.umass.cs.sqliteBenchmarking;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;

import java.sql.SQLException;


import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DataSource
{
    //private static DataSource     datasource;
    private ComboPooledDataSource cpds;
    
    //private final int portNum;
    
    //private final String dirName;
    
    private final String dbName;
    
    public DataSource(int nodeId) throws IOException, SQLException, PropertyVetoException
    {
    	//portNum = 6000;
    	
    	//dirName = "mysqlDir-serv";
    	
    	dbName = "contextDB.db";
    	
    	
        cpds = new ComboPooledDataSource();
        cpds.setDriverClass("org.sqlite.JDBC"); //loads the jdbc driver
        //cpds.setJdbcUrl("jdbc:sqlite:"+"/home/ayadav/"+dbName);
        
        cpds.setJdbcUrl("jdbc:sqlite::memory:");
        
        // the settings below are optional -- c3p0 can work with defaults
        //cpds.setMinPoolSize(5);
        //cpds.setAcquireIncrement(5);
        // 151 is default but on d710 machines it is set to 214
        cpds.setMaxPoolSize(1);
        //cpds.setAutoCommitOnClose(false);
        //cpds.setMaxStatements(180);
    }

//    public static DataSource getInstance() throws IOException, SQLException, PropertyVetoException 
//    {
//    	if (datasource == null) 
//        {
//    		datasource = new DataSource();
//    		return datasource;
//        } else 
//        {
//        	return datasource;
//        }
//    }

    public Connection getConnection() throws SQLException 
    {
    	return this.cpds.getConnection();
    }
    
//    private void createDB()
//    {
//    	Connection conn = null;
//    	Statement stmt = null;
//    	try
//    	{
//    		//STEP 2: Register JDBC driver
//    		Class.forName("com.mysql.jdbc.Driver");
//    		//int portNum = 6000;
//        	//String dirName = "mysqlDir-serv0";
//        	
//    		//STEP 3: Open a connection
//    		String jdbcURL = "jdbc:mysql://localhost:"
//    						+portNum+"?socket=/home/"+dirName+"/thesock";
//    		
//    	    //ContextServiceLogger.getLogger().fine("Connecting to database...");
//    	    conn = DriverManager.getConnection(jdbcURL, "root", "aditya");
//    	    
//		    //STEP 4: Execute a query
//		    //ContextServiceLogger.getLogger().fine("Creating database...");
//		    stmt = conn.createStatement();
//		    String sql = "CREATE DATABASE testDB";
//		    stmt.executeUpdate(sql);
//		}
//    	catch(SQLException se)
//    	{
//    		se.printStackTrace();
//    	}catch(Exception e)
//    	{
//    		e.printStackTrace();
//    	}
//    	finally
//    	{
//    		//finally block used to close resources
//    	    try
//    	    {
//    	    	if(stmt!=null)
//    	    		stmt.close();
//    	    }
//    	    catch(SQLException se2)
//    	    {
//    	    }// nothing we can do
//    	    try
//    	    {
//    	    	if(conn!=null)
//    	    		conn.close();
//    	    }
//    	    catch(SQLException se)
//    	    {
//    	    	se.printStackTrace();
//    	    }//end finally try
//    	}//end try
//    }
//    
//    private void dropDB()
//    {
//    	Connection conn = null;
//    	Statement stmt = null;
//    	try
//    	{
//    		Class.forName("com.mysql.jdbc.Driver");
////    		int portNum = 6000;
////        	String dirName = "mysqlDir-serv0";
//        	
//    		String jdbcURL = "jdbc:mysql://localhost:"
//    							+portNum+"?socket=/home/"+dirName+"/thesock";
//    		
//    	    conn = DriverManager.getConnection(jdbcURL, "root", "aditya");
//    	    
//		    stmt = conn.createStatement();
//		    String sql = "drop DATABASE testDB";
//		    stmt.executeUpdate(sql);
//		}
//    	catch(SQLException se)
//    	{
//    		se.printStackTrace();
//    	}catch(Exception e)
//    	{
//    		e.printStackTrace();
//    	}finally
//    	{
//    		//finally block used to close resources
//    	    try
//    	    {
//    	    	if(stmt!=null)
//    	    		stmt.close();
//    	    }
//    	    catch(SQLException se2)
//    	    {
//    	    }// nothing we can do
//    	    try
//    	    {
//    	    	if(conn!=null)
//    	    		conn.close();
//    	    }
//    	    catch(SQLException se)
//    	    {
//    	    	se.printStackTrace();
//    	    }//end finally try
//    	}//end try
//    }
}