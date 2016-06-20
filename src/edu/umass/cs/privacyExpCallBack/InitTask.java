package edu.umass.cs.privacyExpCallBack;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;

import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.GuidEntry;


public class InitTask implements Runnable
{
	private final int guidNum;
	private final KeyPairGenerator kpg;	
	
	public InitTask(int guidNum) throws NoSuchAlgorithmException
	{
		this.guidNum = guidNum;
		kpg = KeyPairGenerator.getInstance( "RSA" );
	}
	
	@Override
	public void run() 
	{
		String alias = "GUID"+guidNum;
		KeyPair kp0 = kpg.generateKeyPair();
		PublicKey publicKey0 = kp0.getPublic();
		PrivateKey privateKey0 = kp0.getPrivate();
		byte[] publicKeyByteArray0 = publicKey0.getEncoded();
		
		String guid0 = Utils.convertPublicKeyToGUIDString(publicKeyByteArray0);
		GuidEntry myGUID = new GuidEntry(alias, guid0, 
				publicKey0, privateKey0);
		
		UserEntry userEntry = new UserEntry(myGUID);
		
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
}