package edu.umass.cs.nwsdataprocessing;

import java.io.File;
import java.io.IOException;

public class ConvertFetchedDataIntoGeoJSON
{
	public static final String RAW_WEATHER_DATA_DIR				= "/home/ayadav/Documents/Data/weatherDataSnapShot/rawdata";
	
	public static final String GEOJSON_WEATHER_DATA_DIR			= "/home/ayadav/Documents/Data/weatherDataSnapShot/geojsondata";
	
	public static final String SCRIPT_DIR						= "/home/ayadav/Documents/Data/weatherDataSnapShot";
	
	
	public static void main(String[] args)
	{
		File folder = new File(RAW_WEATHER_DATA_DIR);
		File[] listOfFiles = folder.listFiles();
		
		for (int i = 0; i < listOfFiles.length; i++)
		{
			if( listOfFiles[i].isFile() )
			{
				System.out.println("File " + listOfFiles[i].getName());
				
				String filename = listOfFiles[i].getName();
				String sourceFilePath = RAW_WEATHER_DATA_DIR+"/"+filename;
				String justfname = filename.split(".zip")[0];
				
				//String destFilePath = GEOJSON_WEATHER_DATA_DIR + "/"+justfname+".json";
				String destFilePath = justfname+".json";
				
				try 
				{
					Process p = Runtime.getRuntime().exec
						("bash "+SCRIPT_DIR+"/extractAndStoreGeoJson.sh "+sourceFilePath+" "+destFilePath);
					p.waitFor();
				}
				catch (IOException e) 
				{
					e.printStackTrace();
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
			else if (listOfFiles[i].isDirectory())
			{
				//System.out.println("Directory " + listOfFiles[i].getName());
		    }
		}
	}
}