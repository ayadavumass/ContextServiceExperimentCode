package edu.umass.cs.privacyExp2WithGNSCallBack;


import java.security.NoSuchAlgorithmException;

import org.json.JSONArray;

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.GNSCommandProtocol;


public class InitTask implements Runnable
{
	private final String guidAlias;
	
	public InitTask(String guidAlias) throws NoSuchAlgorithmException
	{
		this.guidAlias = guidAlias;
	}
	
	@Override
	public void run() 
	{
		try 
		{
			GNSClient gnsClient = SearchAndUpdateDriver.csClient.getGNSClient();
		
			GuidEntry userGUID = GuidUtils.lookupOrCreateAccountGuid
				(gnsClient, guidAlias, "password");
			
			// resetting SYMMETRIC_KEY_EXCHANGE_FIELD_NAME field.
			gnsClient.execute( GNSCommand.fieldReplaceOrCreateList
					(userGUID, 
					ContextServiceClient.SYMMETRIC_KEY_EXCHANGE_FIELD_NAME, 
					new JSONArray()));
			
			// any GUID can append symmetric key information here.
			gnsClient.execute( GNSCommand.aclAdd(AclAccessType.WRITE_WHITELIST, userGUID, 
					ContextServiceClient.SYMMETRIC_KEY_EXCHANGE_FIELD_NAME, GNSCommandProtocol.ALL_GUIDS) );
			
			UserEntry userEntry = new UserEntry(userGUID);
			
			synchronized(SearchAndUpdateDriver.usersVector)
			{
				SearchAndUpdateDriver.usersVector.add(userEntry);
				
				if( SearchAndUpdateDriver.usersVector.size() == 
						SearchAndUpdateDriver.numUsers )
				{
					SearchAndUpdateDriver.usersVector.notify();
				}
			}
		}
		catch (Exception e) 
		{
			System.out.println("GUID alias "+guidAlias+" couldn't be created");
			e.printStackTrace();
		}
	}
}