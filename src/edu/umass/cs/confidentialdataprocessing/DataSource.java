package edu.umass.cs.confidentialdataprocessing;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;

import java.sql.SQLException;


import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DataSource
{
    private ComboPooledDataSource cpds;
    
    private final String dbName;
    
    public DataSource() throws IOException, SQLException, PropertyVetoException
    {
    	dbName = "hazard";
    	
        cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver"); //loads the jdbc driver
        cpds.setJdbcUrl("jdbc:mysql://localhost:3306"+"/"+dbName);
        
        cpds.setUser("root");
        cpds.setPassword("aditya");
        
        // the settings below are optional -- c3p0 can work with defaults
        //cpds.setMinPoolSize(5);
        //cpds.setAcquireIncrement(5);
        // 151 is default but on d710 machines it is set to 214
        cpds.setMaxPoolSize(10);
        cpds.setAutoCommitOnClose(false);
        //cpds.setMaxStatements(180);
    }
    
    public Connection getConnection() throws SQLException 
    {
    	return this.cpds.getConnection();
    }
}