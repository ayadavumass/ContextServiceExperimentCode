package edu.umass.cs.microsoftBeijingData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Get number of trajectories and number of lines for each
 * user
 * @author adipc
 */
public class GetNumTrajectoriesAndLines
{
	public static final String dirName 
				= "/home/adipc/Documents/GeoLifeData/GeolifeTrajectories1.3/Data";
	
	public static final double minBLat  = 39.00;
	public static final double maxBLat  = 41.00;
	
	public static final double minBLong = 115.50;
	public static final double maxBLong = 117.50;
	
	
	public static HashMap<Integer, UserInfo> numTrajectoriesMap 
												= new HashMap<Integer, UserInfo>();
	
	
//	public void listFilesForFolder( File folder )
//	{
//		for ( File fileEntry : folder.listFiles() )
//		{
//			if ( fileEntry.isDirectory() )
//			{
//	           //listFilesForFolder(fileEntry);
//	        }
//			else
//			{
//	            System.out.println(fileEntry.getName());
//	            
//	        }
//	    }
//	}
	
	public static void readAllTrajectories
							(UserInfo userInfo, String trajFolder )
	{
		File dataFolder = new File(trajFolder);
		
		for ( File fileEntry : dataFolder.listFiles() )
		{
			String fileName = fileEntry.getName();
			String path = trajFolder+"/"+fileName;
			
			readTrajLineByLine( fileName, path, userInfo );
		}
	}
	
	public static void readTrajLineByLine
					( String trajFileName, String trajFilePath, UserInfo userInfo )
	{
		BufferedReader br = null;
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader( new FileReader(trajFilePath) );
			long currLines = 0;
			boolean trajInside = true;
			while ( (sCurrentLine = br.readLine()) != null )
			{
				currLines++;
				if( currLines <= 6)
				{
					continue;
				}
				
				String[] parsed = sCurrentLine.split(",");
				
				double latitude = Double.parseDouble(parsed[0]);
				double longitude = Double.parseDouble(parsed[1]);
				
				boolean inside = ifLatLongInArea(latitude, longitude);
				
				if( !inside )
				{
					trajInside = inside;
					break;
				}
			}
			
			if( trajInside )
			{
				userInfo.numberOfLinesTrajMap.put(trajFileName, currLines);
			}
		} catch (IOException e)
		{
			e.printStackTrace();
		} finally
		{
			try
			{
				if ( br != null )
					br.close();
			}
			catch ( IOException ex )
			{
				ex.printStackTrace();
			}
		}
	}
	
	public static boolean ifLatLongInArea(double latitude, double longitude)
	{
		return true;
//		if( (latitude >= minBLat) && (latitude <= maxBLat) 
//				&& (longitude >= minBLong) && (longitude <= maxBLong) )
//		{
//			return true;
//		}
//		else
//		{
//			return false;
//		}
	}
	
	public static void readAndInitialize()
	{
		File dataFolder = new File(dirName);
		
		for ( File fileEntry : dataFolder.listFiles() )
		{
			if ( fileEntry.isDirectory() )
			{
	           //listFilesForFolder(fileEntry);
	        }
			else
			{
				String userIdStr = fileEntry.getName();
				UserInfo uInfo   = new UserInfo();
				
				int userId = Integer.parseInt(userIdStr);
				
				numTrajectoriesMap.put(userId, uInfo);
				
				String trajFolder = dataFolder+"/"+userIdStr;
				
				readAllTrajectories(uInfo, trajFolder );
	        }
	    }
	}
	
	public static void main(String[] args)
	{
		readAndInitialize();
		
		System.out.println("numTrajectoriesMap "+numTrajectoriesMap.size());
	}
}