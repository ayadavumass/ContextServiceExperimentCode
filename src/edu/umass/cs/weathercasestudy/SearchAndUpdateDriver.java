package edu.umass.cs.weathercasestudy;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class SearchAndUpdateDriver 
{
	public static String csHost			= null;
	public static int csPort			= -1;
	public static int NUMUSERS			= -1;
	
	public static void main( String[] args )
	{
		csHost = args[0];
		csPort = Integer.parseInt(args[1]);
		NUMUSERS = Integer.parseInt(args[2]);
		
		new Thread(new MobilityThread()).start();
		new Thread(new WeatherThread()).start();
		
	}
	
	public static class MobilityThread implements Runnable
	{
		@Override
		public void run()
		{
			String[] args = {csHost, csPort+"", NUMUSERS+""};
			try
			{
				IssueUpdates.main(args);
			}
			catch (NoSuchAlgorithmException | IOException | InterruptedException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	public static class WeatherThread implements Runnable
	{
		@Override
		public void run()
		{
			String[] args = {csHost, csPort+""};
			try 
			{
				IssueSearches.main(args);
			}
			catch (NoSuchAlgorithmException | IOException | InterruptedException e) 
			{
				e.printStackTrace();
			}
		}
	}
}