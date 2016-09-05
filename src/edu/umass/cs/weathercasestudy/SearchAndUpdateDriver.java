package edu.umass.cs.weathercasestudy;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class SearchAndUpdateDriver 
{
	public static String csHost			= null;
	public static int csPort			= -1;
	public static int NUMUSERS			= -1;
	
	public static void main( String[] args ) throws NoSuchAlgorithmException, IOException
	{
		csHost = args[0];
		csPort = Integer.parseInt(args[1]);
		NUMUSERS = Integer.parseInt(args[2]);
		
		IssueUpdates issUpd = new IssueUpdates(csHost, csPort, NUMUSERS);
		issUpd.readNomadLag();
		issUpd.createTransformedTrajectories();
		
//		System.out.println("minLatData "+issUpd.minLatData+" maxLatData "+issUpd.maxLatData
//				+" minLongData "+issUpd.minLongData+" maxLongData "+issUpd.maxLongData);
		issUpd.printLogStats();
		System.out.println("\n\n");
		issUpd.printRealUserStats();
		
		
		IssueSearches issueSearch = new IssueSearches(csHost, csPort);
		
		
		//issUpd.runUpdates();
		//issueSearch.runSearches();
		
		
		new Thread(new MobilityThread(issUpd)).start();
		new Thread(new WeatherThread(issueSearch)).start();
		
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
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
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