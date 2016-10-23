package edu.umass.cs.benchmarking;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;

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
			RSAPublicKey publicKey0 = (RSAPublicKey) kp0.getPublic();
			RSAPrivateKey privateKey0 = (RSAPrivateKey) kp0.getPrivate();
			int len = publicKey0.getModulus().bitLength();
			System.out.println("key len "+len+ " "+privateKey0.getEncoded().length);
		}
		
		long end = System.currentTimeMillis();
		
		System.out.println("Time taken to generate "+numKeys+" keys "+(end-start)+" ms");
	}
}