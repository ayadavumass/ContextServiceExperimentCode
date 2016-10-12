package edu.umass.cs.modelParameters;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DataSource
{
    private static DataSource     datasource;
    private ComboPooledDataSource cpds;

    
    public DataSource() throws IOException, SQLException, PropertyVetoException
    {
    	int portNum = 6000+ThroughputMeasure.nodeId;
    	String dirName = "mysqlDir-serv"+ThroughputMeasure.nodeId;
        cpds = new ComboPooledDataSource();
        //dropDB();
        //createDB();
        cpds.setDriverClass("com.mysql.jdbc.Driver"); //loads the jdbc driver
        cpds.setJdbcUrl("jdbc:mysql://localhost:"+portNum+"/contextDB"+ThroughputMeasure.nodeId
        		+"?socket=/home/"+dirName+"/thesock");
        cpds.setUser("root");
        cpds.setPassword("aditya");
        
        // the settings below are optional -- c3p0 can work with defaults
        //cpds.setMinPoolSize(5);
        //cpds.setAcquireIncrement(5);
        //cpds.setMaxPoolSize(100);
        //cpds.setMaxStatements(180);
    }

    public static DataSource getInstance() throws IOException, SQLException, PropertyVetoException 
    {
        if (datasource == null) 
        {
            datasource = new DataSource();
            return datasource;
        } else 
        {
            return datasource;
        }
    }

    public Connection getConnection() throws SQLException 
    {
        return this.cpds.getConnection();
    }
}

/*private void createDB()
{
	Connection conn = null;
	Statement stmt = null;
	try
	{
		//STEP 2: Register JDBC driver
		Class.forName("com.mysql.jdbc.Driver");
		int portNum = 6000;
    	String dirName = "mysqlDir-serv0";
    	
		//STEP 3: Open a connection
		String jdbcURL = "jdbc:mysql://localhost:"+portNum+"?socket=/home/"+dirName+"/thesock";
		
		
	    //ContextServiceLogger.getLogger().fine("Connecting to database...");
	    conn = DriverManager.getConnection(jdbcURL, "root", "aditya");

	    //STEP 4: Execute a query
	    //ContextServiceLogger.getLogger().fine("Creating database...");
	    stmt = conn.createStatement();
	    String sql = "CREATE DATABASE contextDB";
	    stmt.executeUpdate(sql);
	}
	catch(SQLException se)
	{
		se.printStackTrace();
	}catch(Exception e)
	{
		e.printStackTrace();
	}finally
	{
		//finally block used to close resources
	    try
	    {
	    	if(stmt!=null)
	    		stmt.close();
	    }
	    catch(SQLException se2)
	    {
	    }// nothing we can do
	    try
	    {
	    	if(conn!=null)
	    		conn.close();
	    }
	    catch(SQLException se)
	    {
	    	se.printStackTrace();
	    }//end finally try
	}//end try
}

private void dropDB()
{
	Connection conn = null;
	Statement stmt = null;
	try
	{
		//STEP 2: Register JDBC driver
		Class.forName("com.mysql.jdbc.Driver");
		int portNum = 6000;
    	String dirName = "mysqlDir-serv0";
    	
		//STEP 3: Open a connection
		String jdbcURL = "jdbc:mysql://localhost:"+portNum+"?socket=/home/"+dirName+"/thesock";
		
		
	    //ContextServiceLogger.getLogger().fine("Connecting to database...");
	    conn = DriverManager.getConnection(jdbcURL, "root", "aditya");

	    //STEP 4: Execute a query
	    //ContextServiceLogger.getLogger().fine("Creating database...");
	    stmt = conn.createStatement();
	    String sql = "drop DATABASE contextDB";
	    stmt.executeUpdate(sql);
	}
	catch(SQLException se)
	{
		se.printStackTrace();
	}catch(Exception e)
	{
		e.printStackTrace();
	}finally
	{
		//finally block used to close resources
	    try
	    {
	    	if(stmt!=null)
	    		stmt.close();
	    }
	    catch(SQLException se2)
	    {
	    }// nothing we can do
	    try
	    {
	    	if(conn!=null)
	    		conn.close();
	    }
	    catch(SQLException se)
	    {
	    	se.printStackTrace();
	    }//end finally try
	}//end try
}*/