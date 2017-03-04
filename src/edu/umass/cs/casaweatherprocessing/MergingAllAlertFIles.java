package edu.umass.cs.casaweatherprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class MergingAllAlertFIles 
{
	public static final String ALERT_FILE_DIR		= "/home/ayadav/Documents/Data/casaweatherdata/alerts";
	public static final String MERGE_FILE_DIR		= "/home/ayadav/Documents/Data/casaweatherdata";
	
	private static void mergeAllAlertsInOneFile()
	{
		File folder = new File(ALERT_FILE_DIR);
		File[] listOfFiles = folder.listFiles();
		
		BufferedWriter bw = null;
		
		
		try
		{
			bw = new BufferedWriter(new FileWriter(MERGE_FILE_DIR+"/mergedAlerts"));
			
			for (int i = 0; i < listOfFiles.length; i++)
			{
				if ( listOfFiles[i].isFile() )
				{
					String filename = listOfFiles[i].getName();
					
					BufferedReader br = null;
					try
					{
						br = new BufferedReader(
								new FileReader(ALERT_FILE_DIR+"/"+filename));
						
						String sCurrentLine;
						while( (sCurrentLine = br.readLine()) != null )
						{
							bw.write(sCurrentLine+"\n");
						}
					}
					catch(IOException ioex)
					{
						ioex.printStackTrace();
					}
					finally
					{
						if(br != null)
						{
							br.close();
						}
					}
				}
				else if (listOfFiles[i].isDirectory())
				{
					//assert(false);
					System.out.println("Directory " + listOfFiles[i].getName());
			    }
			}
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
		}
		finally
		{
			if(bw != null)
			{
				try 
				{
					bw.close();
				} 
				catch (IOException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void main(String[] args)
	{
		mergeAllAlertsInOneFile();
	}
}