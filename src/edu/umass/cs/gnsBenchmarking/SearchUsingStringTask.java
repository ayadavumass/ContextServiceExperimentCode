package edu.umass.cs.gnsBenchmarking;

import java.util.List;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;

public class SearchUsingStringTask implements Runnable
{
	private final String searchQuery;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public SearchUsingStringTask( String searchQuery, 
			AbstractRequestSendingClass requestSendingTask )
	{
		this.searchQuery = searchQuery;
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
			GNSClient gnsClient = SearchAndUpdateDriver.getGNSClient();
			long start = System.currentTimeMillis();
			
			//System.out.println("Sending search "+searchQuery);
			List<String> guidList  
				= (List<String>) gnsClient.execute
			(GNSCommand.selectQuery(searchQuery)).getResultList();
			
			long end = System.currentTimeMillis();
			requestSendingTask.incrementSearchNumRecvd
											(guidList.size(), end-start);
			SearchAndUpdateDriver.returnGNSClient(gnsClient);
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