package edu.umass.cs.genericExpClient;

import org.json.JSONObject;

/**
 * Class implements the task used for 
 * update info in GNS, which is blocking so this 
 * class's object is passed in executor service
 * @author adipc
 */
public class UpdateTask implements Runnable
{
	private final JSONObject attrValuePairs;
	private final String userGUID;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public UpdateTask( JSONObject attrValuePairs, String userGUID,
			AbstractRequestSendingClass requestSendingTask )
	{
		this.attrValuePairs = attrValuePairs;
		this.userGUID = userGUID;
		this.requestSendingTask = requestSendingTask;
	}
	
	@Override
	public void run()
	{
		try
		{
			if( SearchAndUpdateDriver.useGNS )
			{
//				GuidEntry guidEntry = userRecordInfo.getUserGuidEntry();
//				long start = System.currentTimeMillis();
//				SearchAndUpdateDriver.gnsClient.update(guidEntry.getGuid(), attrValuePairs, guidEntry);
//				long end = System.currentTimeMillis();
//				requestSendingTask.incrementUpdateNumRecvd(guidEntry.getGuid(), end-start);
			}
			else
			{
				System.out.println("Sending update userGUID "
									+userGUID+" attrValuePairs "+attrValuePairs);
				long start = System.currentTimeMillis();
				SearchAndUpdateDriver.csClient.sendUpdate(userGUID, null, 
						attrValuePairs, -1);
				long end = System.currentTimeMillis();
				requestSendingTask.incrementUpdateNumRecvd(userGUID, end-start);
			}
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