package edu.umass.cs.genericExpClientCallBack;

import org.json.JSONArray;

public class SearchTask implements Runnable
{
	private final String searchQuery;
	private final JSONArray replybackArray;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public SearchTask( String searchQuery, JSONArray replybackArray, 
			AbstractRequestSendingClass requestSendingTask )
	{
		this.searchQuery = searchQuery;
		this.replybackArray = replybackArray;
		this.requestSendingTask = requestSendingTask;
	}
	
	@Override
	public void run()
	{
		try
		{
//			ExperimentSearchReply searchRep 
//								= new ExperimentSearchReply( reqId );
			
//			SearchAndUpdateDriver.csClient.sendSearchQueryWithCallBack
//								(searchQuery, 300000, searchRep, requestSendingTask.getCallBack());
			
			long start = System.currentTimeMillis();
			
			int replySize 
				= SearchAndUpdateDriver.csClient.sendSearchQuery
				(searchQuery, replybackArray, 300000);
			
			long end = System.currentTimeMillis();
			requestSendingTask.incrementSearchNumRecvd
											(replySize, end-start);
		} 
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		catch(Error ex)
		{
			ex.printStackTrace();
		}
	}
}