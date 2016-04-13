package edu.umass.cs.weatherExpClient;

import java.io.IOException;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GuidEntry;

/**
 * Class implements the task used for 
 * update info in GNS, which is blocking so this 
 * class's object is passed in executor service
 * @author adipc
 */
public class UpdateTask implements Runnable
{
	private final JSONObject attrValuePairs;
	private final UserRecordInfo userRecordInfo;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public UpdateTask(JSONObject attrValuePairs, UserRecordInfo userRecordInfo, AbstractRequestSendingClass requestSendingTask)
	{
		this.attrValuePairs = attrValuePairs;
		this.userRecordInfo = userRecordInfo;
		this.requestSendingTask = requestSendingTask;
	}
	
	@Override
	public void run()
	{
		try
		{
			if( WeatherAndMobilityBoth.useGNS )
			{
				GuidEntry guidEntry = userRecordInfo.getUserGuidEntry();
				long start = System.currentTimeMillis();
				WeatherAndMobilityBoth.gnsClient.update(guidEntry.getGuid(), attrValuePairs, guidEntry);
				long end = System.currentTimeMillis();
				requestSendingTask.incrementUpdateNumRecvd(guidEntry.getGuid(), end-start);
			}
			else
			{
				String userGUID = userRecordInfo.getGUIDString();
				long start = System.currentTimeMillis();
			
				WeatherAndMobilityBoth.csClient.sendUpdate
								(userGUID, null, attrValuePairs, -1, true);
				long end = System.currentTimeMillis();
				requestSendingTask.incrementUpdateNumRecvd(userGUID, end-start);
			}
		} catch (IOException e)
		{
			e.printStackTrace();
		}  catch(Exception ex)
		{
			ex.printStackTrace();
		}
		catch(Error ex)
		{
			ex.printStackTrace();
		}
	}
}