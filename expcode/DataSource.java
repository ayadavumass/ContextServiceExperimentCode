package edu.umass.cs.expcode;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;


import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DataSource 
{
    private static DataSource     datasource;
    private ComboPooledDataSource cpds;

    DataSource() throws IOException, SQLException, PropertyVetoException 
    {
    	int portNum = 6000;
    	String dirName = "mysqlDir-compute-0-13";
    	
        cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver"); //loads the jdbc driver
        cpds.setJdbcUrl("jdbc:mysql://localhost:"+portNum+"/contextDB?socket=/home/ayadav/"+dirName+"/thesock");
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