package edu.umass.cs.hyperdexExperiments;

import java.util.Map;

import org.hyperdex.client.Client;
import org.json.JSONArray;

public class SearchTask implements Runnable
{
	//private final String searchQuery;
	private final JSONArray replybackArray;
	private final AbstractRequestSendingClass requestSendingTask;
	
	private final Map<String, Object> searchQueryMap;
	
	public SearchTask( Map<String, Object> searchQueryMap, JSONArray replybackArray, 
			AbstractRequestSendingClass requestSendingTask )
	{
		this.searchQueryMap = searchQueryMap;
		this.replybackArray = replybackArray;
		this.requestSendingTask = requestSendingTask;
	}
	
	@Override
	public void run()
	{
		try
		{
			Client hClient = SearchAndUpdateDriver.getHyperdexClient();	
			
			long start = System.currentTimeMillis();
			long numReplies = hClient.count( SearchAndUpdateDriver.HYPERSPACE_NAME, 
					searchQueryMap );
			
			long end = System.currentTimeMillis();
			requestSendingTask.incrementSearchNumRecvd
											((int)numReplies, end-start);
			
			
//			ExperimentSearchReply searchRep 
//								= new ExperimentSearchReply( reqId );
			
//			SearchAndUpdateDriver.csClient.sendSearchQueryWithCallBack
//								(searchQuery, 300000, searchRep, requestSendingTask.getCallBack());
			
			//long start = System.currentTimeMillis();
			
			
//			int replySize 
//				= SearchAndUpdateDriver.csClient.sendSearchQuery
//				(searchQuery, replybackArray, 300000);
//			
//			long end = System.currentTimeMillis();
//			requestSendingTask.incrementSearchNumRecvd
//											(replySize, end-start);
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