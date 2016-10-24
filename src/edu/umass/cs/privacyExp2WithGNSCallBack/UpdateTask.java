package edu.umass.cs.privacyExp2WithGNSCallBack;


import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.util.GuidEntry;


/**
 * Class implements the task used for 
 * update info in GNS, which is blocking so this 
 * class's object is passed in executor service
 * @author adipc
 */
public class UpdateTask implements Runnable
{
	private final JSONObject attrValuePairs;
	private final UserEntry userEntry;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public UpdateTask( JSONObject attrValuePairs, UserEntry userEntry, 
			AbstractRequestSendingClass requestSendingTask )
	{
		this.attrValuePairs = attrValuePairs;
		this.userEntry = userEntry;
		this.requestSendingTask = requestSendingTask;
	}
	
	@Override
	public void run()
	{
		try
		{
			System.out.println(" Sending update userGUID "
								+userEntry.getGuidEntry().getGuid()
								+" attrValuePairs "+attrValuePairs );
			
			long start = System.currentTimeMillis();
			
			GuidEntry myGUIDInfo = userEntry.getGuidEntry();
			String guidString = userEntry.getGuidEntry().getGuid();
			
			
			SearchAndUpdateDriver.csClient.sendUpdateSecure( 
					guidString, myGUIDInfo, attrValuePairs, -1, 
					userEntry.getACLMap(), userEntry.getAnonymizedIDList() );
			
			long end = System.currentTimeMillis();
			requestSendingTask.incrementUpdateNumRecvd(guidString, end-start);
			
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