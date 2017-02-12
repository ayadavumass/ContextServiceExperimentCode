package edu.umass.cs.gnsBenchmarking;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;

/**
 * Class implements the task used for 
 * update info in GNS, which is blocking so this 
 * class's object is passed in executor service
 * @author adipc
 */
public class UpdateTask implements Runnable
{
	private final JSONObject attrValuePairs;
	private final String guidAlias;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public UpdateTask( JSONObject attrValuePairs, String guidAlias,
			AbstractRequestSendingClass requestSendingTask )
	{
		this.attrValuePairs = attrValuePairs;
		this.guidAlias = guidAlias;
		this.requestSendingTask = requestSendingTask;
	}
	
	@Override
	public void run()
	{
		try
		{
//			System.out.println("Sending update userGUID "
//								+userGUID+" attrValuePairs "+attrValuePairs);
			GNSClient gnsClient = SearchAndUpdateDriver.getGNSClient();
			long start = System.currentTimeMillis();
			GuidEntry guidEntry = GuidUtils.lookupGuidEntryFromDatabase(gnsClient, guidAlias);
			gnsClient.execute(GNSCommand.update(guidEntry, attrValuePairs));
			long end = System.currentTimeMillis();
			requestSendingTask.incrementUpdateNumRecvd(guidEntry.guid, end-start);
			SearchAndUpdateDriver.returnGNSClient(gnsClient);
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