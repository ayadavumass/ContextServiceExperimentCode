package edu.umass.cs.weathercasestudy;

import java.io.IOException;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;


public class UpdateTask implements Runnable
{
	private final JSONObject attrValuePairs;
	private final GuidEntry guidEntry;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public UpdateTask( JSONObject attrValuePairs, GuidEntry guidEntry, 
			AbstractRequestSendingClass requestSendingTask )
	{
		this.attrValuePairs = attrValuePairs;
		this.guidEntry = guidEntry;
		this.requestSendingTask = requestSendingTask;
	}
	
	@Override
	public void run()
	{
		try {
			sendUpdate();
		} catch (ClientException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void sendUpdate() throws ClientException, IOException 
	{	
		GNSClient gnsClient = SearchAndUpdateDriver.getGNSClient();
		long start = System.currentTimeMillis();
		gnsClient.execute(GNSCommand.update(guidEntry, attrValuePairs));
		long end = System.currentTimeMillis();
		requestSendingTask.incrementUpdateNumRecvd(guidEntry.guid, end-start);
		SearchAndUpdateDriver.returnGNSClient(gnsClient);
	}
}