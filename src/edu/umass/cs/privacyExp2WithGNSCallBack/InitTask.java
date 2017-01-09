package edu.umass.cs.privacyExp2WithGNSCallBack;


import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.util.GuidEntry;

public class InitTask implements Runnable
{
	private final String guidAlias;
	private final KeyPairGenerator kpg;
	
	public InitTask(String guidAlias) throws NoSuchAlgorithmException
	{
		this.guidAlias = guidAlias;
		kpg = KeyPairGenerator.getInstance( ContextServiceConfig.AssymmetricEncAlgorithm );
	}
	
	@Override
	public void run() 
	{
		try 
		{
			KeyPair kp0 = kpg.generateKeyPair();
			PublicKey publicKey0 = kp0.getPublic();
			PrivateKey privateKey0 = kp0.getPrivate();
			byte[] publicKeyByteArray0 = publicKey0.getEncoded();
			
			String guid0 = Utils.convertPublicKeyToGUIDString(publicKeyByteArray0);
			
			GuidEntry userGUID =  new GuidEntry(guidAlias, guid0, 
						publicKey0, privateKey0);
				
			//GNSClient gnsClient = SearchAndUpdateDriver.csClient.getGNSClient();
		
//			GuidEntry userGUID = GuidUtils.lookupOrCreateAccountGuid
//				(gnsClient, guidAlias, "password");
			
			// resetting SYMMETRIC_KEY_EXCHANGE_FIELD_NAME field.
//			gnsClient.execute( GNSCommand.fieldReplaceOrCreateList
//					(userGUID, 
//					ContextServiceClient.SYMMETRIC_KEY_EXCHANGE_FIELD_NAME, 
//					new JSONArray()));
			
			// any GUID can append symmetric key information here.
			//gnsClient.execute( GNSCommand.aclAdd(AclAccessType.WRITE_WHITELIST, userGUID, 
			//		ContextServiceClient.SYMMETRIC_KEY_EXCHANGE_FIELD_NAME, GNSCommandProtocol.ALL_GUIDS) );
			
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