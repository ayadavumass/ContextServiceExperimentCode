package edu.umass.cs.confidentialdataprocessing;

import java.beans.PropertyVetoException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;



/**
 * This class extracts JSON from confidential mysql data of casa weather app
 * users.
 * @author ayadav
 *
 */
public class ExtractJSONFromMySQL 
{	
	public static final String confidentialJSONFile 
			= "/home/ayadav/Documents/Data/confidentialUserTraces/confidentialJSON.txt";
	
	public static void main(String[] args) 
					throws IOException, SQLException, PropertyVetoException
	{
		BufferedWriter bw = null; 
		
		String searchQuery = "select json from `userArchive2016-2017scrub`";
		
		DataSource ds = new DataSource();
		
		Connection myConn = null;
		Statement stmt = null;
		
		try
		{
			bw = new BufferedWriter(new FileWriter(confidentialJSONFile));
			
			myConn = ds.getConnection();
			//stmt = myConn.createStatement();
			
			
			stmt   = myConn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, 
					java.sql.ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(Integer.MIN_VALUE);
				
			ResultSet rs = stmt.executeQuery(searchQuery);
			
			System.out.println("Got the result from the DB. Now writing to file");
			while( rs.next() )
			{
				//Retrieve by column name
				//double value  	 = rs.getDouble("value");
				String jsonString  = rs.getString("json");
				bw.write(jsonString+"\n");
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
				 
				 if(bw != null)
					 bw.close();
				 
			 } catch (SQLException e) 
			 {
				 e.printStackTrace();
			 }
		}
	}
	
}