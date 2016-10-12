package edu.umass.cs.benchmarking;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;

public class RSAKeyCreationBenchmarking 
{
	
	public static void main(String[] args) throws NoSuchAlgorithmException
	{
		int numKeys = Integer.parseInt(args[0]);
		
		KeyPairGenerator kpg;
		kpg = KeyPairGenerator.getInstance("RSA");
		
		long start = System.currentTimeMillis();
		for( int i=0; i< numKeys; i++)
		{
			
			KeyPair kp0 = kpg.genKeyPair();
			PublicKey publicKey0 = kp0.getPublic();
			PrivateKey privateKey0 = kp0.getPrivate();
		}
		
		long end = System.currentTimeMillis();
		
		System.out.println("Time taken to generate "+numKeys+" keys "+(end-start)+" ms");
	}
}