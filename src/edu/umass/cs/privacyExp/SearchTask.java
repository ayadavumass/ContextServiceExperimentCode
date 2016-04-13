package edu.umass.cs.privacyExp;

import org.json.JSONArray;

import edu.umass.cs.gnsclient.client.GuidEntry;

public class SearchTask implements Runnable
{
	private final String searchQuery;
	private final JSONArray replybackArray;
	private GuidEntry queryingGuidEntry;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public SearchTask( String searchQuery, JSONArray replybackArray, GuidEntry queryingGuidEntry, 
			AbstractRequestSendingClass requestSendingTask )
	{
		this.searchQuery = searchQuery;
		this.replybackArray = replybackArray;
		this.queryingGuidEntry = queryingGuidEntry;
		this.requestSendingTask = requestSendingTask;
	}
	
	@Override
	public void run()
	{
		try
		{
			long start = System.currentTimeMillis();
			int replySize = SearchAndUpdateDriver.csClient.sendSearchQuerySecure
					(searchQuery, replybackArray, 300000, queryingGuidEntry);
			//int replySize = SearchAndUpdateDriver.csClient.sendSearchQuery(searchQuery, replybackArray, 300000);
			long end = System.currentTimeMillis();
			requestSendingTask.incrementSearchNumRecvd(replySize, end-start);
		} catch(Exception ex)
		{
			ex.printStackTrace();
		}
		catch(Error ex)
		{
			ex.printStackTrace();
		}
	}
}