package edu.umass.cs.sqliteBenchmarking;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;

import java.sql.SQLException;


import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DataSource
{
	public static final int UPDATE_POOL				= 1;
	public static final int SEARCH_POOL				= 2;
	
    //private static DataSource     datasource;
    private ComboPooledDataSource updatePool;
    
    private ComboPooledDataSource searchPool;
    
    //private final int portNum;
    
    //private final String dirName;
    
    //private final String dbName;
    
    public DataSource() throws IOException, SQLException, PropertyVetoException
    {
//    	updatePool = new ComboPooledDataSource();
//    	updatePool.setDriverClass("org.sqlite.JDBC"); //loads the jdbc driver
//        //cpds.setJdbcUrl("jdbc:sqlite:"+"/home/ayadav/"+dbName);
//        
//    	//updatePool.setJdbcUrl("jdbc:sqlite::memory:");
//    	
//    	updatePool.setJdbcUrl("jdbc:sqlite:file:memdb1?mode=memory&cache=shared");
//        
//    	
//        // the settings below are optional -- c3p0 can work with defaults
//        //cpds.setMinPoolSize(5);
//        //cpds.setAcquireIncrement(5);
//        // 151 is default but on d710 machines it is set to 214
//    	updatePool.setMaxPoolSize(10);
//    	updatePool.setAutoCommitOnClose(false);
//        //cpds.setMaxStatements(180);
    	
    	
    	searchPool = new ComboPooledDataSource();
    	searchPool.setDriverClass("org.sqlite.JDBC"); //loads the jdbc driver
        //cpds.setJdbcUrl("jdbc:sqlite:"+"/home/ayadav/"+dbName);
        
    	//searchPool.setJdbcUrl("jdbc:sqlite::memory:");
        
    	searchPool.setJdbcUrl("jdbc:sqlite:file:memdb1?mode=memory&cache=shared");
    	
        // the settings below are optional -- c3p0 can work with defaults
        //cpds.setMinPoolSize(5);
        //cpds.setAcquireIncrement(5);
        // 151 is default but on d710 machines it is set to 214
    	searchPool.setMaxPoolSize(SQLiteThroughputBenchmarking.PoolSize);
    	searchPool.setAutoCommitOnClose(true);
    }

    public Connection getConnection(int poolType) throws SQLException 
    {
    	return searchPool.getConnection();
//    	if(poolType == UPDATE_POOL)
//    	{
//    		return updatePool.getConnection();
//    	}
//    	else if(poolType == SEARCH_POOL)
//    	{
//    		return searchPool.getConnection();
//    	}
//    	return null;
    }
}