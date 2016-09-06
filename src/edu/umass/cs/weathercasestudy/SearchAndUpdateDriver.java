package edu.umass.cs.weathercasestudy;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class SearchAndUpdateDriver 
{
	public static String csHost			= null;
	public static int csPort			= -1;
	public static int NUMUSERS			= -1;
	public static int myID				= -1;
	public static boolean runSearch		= false;
	
	public static void main( String[] args ) throws NoSuchAlgorithmException, IOException
	{
		csHost = args[0];
		csPort = Integer.parseInt(args[1]);
		NUMUSERS = Integer.parseInt(args[2]);
		myID = Integer.parseInt(args[3]);
		runSearch = Boolean.parseBoolean(args[4]);
		
		
		IssueUpdates issUpd = new IssueUpdates(csHost, csPort, NUMUSERS, myID);
		issUpd.readNomadLag();
		issUpd.createTransformedTrajectories();
		
//		System.out.println("minLatData "+issUpd.minLatData+" maxLatData "+issUpd.maxLatData
//				+" minLongData "+issUpd.minLongData+" maxLongData "+issUpd.maxLongData);
		issUpd.printLogStats();
		System.out.println("\n\n");
		issUpd.printRealUserStats();
		
		
		IssueSearches issueSearch = null;
		if( runSearch )
		{
			issueSearch = new IssueSearches(csHost, csPort);
		}
		
		
		Thread th1 = new Thread(new MobilityThread(issUpd));
		th1.start();
		Thread th2 = null;
		if( runSearch )
		{
			th2 = new Thread(new WeatherThread(issueSearch));
			th2.start();
		}
		
		try 
		{
			th1.join();
		} catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
		
		if(runSearch)
		{
			try
			{
				th2.join();
			} 
			catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
		}
		System.exit(0);
	}
	
	public static class MobilityThread implements Runnable
	{
		private IssueUpdates issUpd;
		
		public MobilityThread(IssueUpdates issUpd)
		{
			this.issUpd = issUpd;
		}
		
		@Override
		public void run()
		{
			try {
				issUpd.runUpdates();
			} catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	public static class WeatherThread implements Runnable
	{
		private IssueSearches issueSearch;
		
		public WeatherThread(IssueSearches issueSearch)
		{
			this.issueSearch = issueSearch;
		}
		
		@Override
		public void run()
		{
			try 
			{
				issueSearch.runSearches();
			}
			catch ( InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}
}