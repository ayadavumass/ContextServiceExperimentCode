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
public class GUIDCreationTask implements Runnable
{
	private final JSONObject attrValuePairs;
	private final String accountGuidAlias;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public GUIDCreationTask( JSONObject attrValuePairs, String accountGuidAlias,
			AbstractRequestSendingClass requestSendingTask )
	{
		this.attrValuePairs = attrValuePairs;
		this.accountGuidAlias = accountGuidAlias;
		this.requestSendingTask = requestSendingTask;
	}
	
	@Override
	public void run()
	{
		GNSClient gnsClient = null;
		try
		{
			long start = System.currentTimeMillis();
			System.out.println("Creating GUID "+accountGuidAlias);
			gnsClient = SearchAndUpdateDriver.getGNSClient();
			System.out.println("Creating GUID "+accountGuidAlias+" Got GNS client");
			GuidEntry guidEntry = GuidUtils.lookupOrCreateAccountGuid
					( gnsClient, accountGuidAlias,
					"password", true );
			
//			GuidEntry guidEntry 
//				= GuidUtils.lookupGuidEntryFromDatabase(gnsClient.getGNSProvider(), 
//						accountGuidAlias);
			
//			synchronized( SearchAndUpdateDriver.guidInsertLock )
//			{
//				SearchAndUpdateDriver.listOfGuidEntries.add(guidEntry);
//			}
			
			gnsClient.execute(GNSCommand.update(guidEntry, attrValuePairs));
			long end = System.currentTimeMillis();
			requestSendingTask.incrementUpdateNumRecvd(guidEntry.guid, end-start);
			
		} catch(Exception ex)
		{
			ex.printStackTrace();
		}
		catch(Error ex)
		{
			ex.printStackTrace();
		}
		finally
		{
			if(gnsClient != null)
			{
				SearchAndUpdateDriver.returnGNSClient(gnsClient);
			}
		}
		
		System.out.println("Creating GUID "+accountGuidAlias+" Finished");
	}
}