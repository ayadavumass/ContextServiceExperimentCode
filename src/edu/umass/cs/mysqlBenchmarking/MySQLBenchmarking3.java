package edu.umass.cs.mysqlBenchmarking;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.utils.DelayProfiler;

public class MySQLBenchmarking3 
{
	public static void main(String[] args) 
	{

		Connection con = null;
		PreparedStatement pst = null;
		
		//String dirName = "mysqlDir-serv0";
		//int portNum = 6000;
		String url = "jdbc:mysql://localhost:3306/contextDB0";
		//String url ="jdbc:mysql://localhost:"+portNum+"/contextDB0?socket=/home/"+dirName+"/thesock";
		String user = "root";
		String password = "aditya";
		

		int n = args.length > 0 ? Integer.valueOf(args[0]) : 1000;

		try 
		{
			con = DriverManager.getConnection(url, user, password);

			pst = con
					.prepareStatement("UPDATE subspaceId0DataStorage SET attr1 = ? WHERE nodeGUID=?");
			
			String guidString = "CCD658B6AD2EA71D28D63C93CF44634B21969463";
			byte[] guidBytes = Utils.hexStringToByteArray(guidString);
			

			for (int i = 0; i < n; i++) {
				long t = System.nanoTime();
				pst.setDouble(1, 1000.0);
				pst.setBytes(2, guidBytes);
				pst.executeUpdate();
				pst.clearParameters();
				DelayProfiler.updateDelayNano("update_latency", t);
			}
			System.out.println(DelayProfiler.getStats());

		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(MySQLBenchmarking3.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);

		} finally {

			try {
				if (pst != null) {
					pst.close();
				}
				if (con != null) {
					con.close();
				}

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(MySQLBenchmarking3.class.getName());
				lgr.log(Level.SEVERE, ex.getMessage(), ex);
			}
		}
	}
}