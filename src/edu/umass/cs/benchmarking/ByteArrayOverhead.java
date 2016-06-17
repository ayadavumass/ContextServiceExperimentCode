package edu.umass.cs.benchmarking;

import java.nio.ByteBuffer;
import java.util.Random;

public class ByteArrayOverhead 
{
	public static final int SIZE_OF_EACH_ELEMENT			= 20+(128*3);
	
	public static void main(String[] args)
	{
		int numGuids = 10000;
		Random rand = new Random(0);
		//Hex hex = new Hex();
		//JSONArray resultJSON = new JSONArray();
		// one GUID and three encrypted element
		
		
		long start = System.currentTimeMillis();
		byte[] byteArray = new byte[SIZE_OF_EACH_ELEMENT*numGuids];
		int offset = 0;
		for(int i=0; i<numGuids; i++)
		{
			byte[] guidBytes = new byte[20];
			rand.nextBytes(guidBytes);
			//String guidString = Utils.bytArrayToHex(guidBytes);
			System.arraycopy(guidBytes, 0, byteArray, offset, guidBytes.length);
			offset = offset + guidBytes.length;
			
			
			byte[] encryptedBytes = new byte[128];
			rand.nextBytes(encryptedBytes);
			
			System.arraycopy(encryptedBytes, 0, byteArray, offset, encryptedBytes.length);
			offset = offset + encryptedBytes.length;
			
			
			encryptedBytes = new byte[128];
			rand.nextBytes(encryptedBytes);
			System.arraycopy(encryptedBytes, 0, byteArray, offset, encryptedBytes.length);
			offset = offset + encryptedBytes.length;
			
			
			encryptedBytes = new byte[128];
			rand.nextBytes(encryptedBytes);
			System.arraycopy(encryptedBytes, 0, byteArray, offset, encryptedBytes.length);
			offset = offset + encryptedBytes.length;
		}
		//buf.flip();
		System.out.println("time taken "+(System.currentTimeMillis() - start));
		
		// time to convert to json tostring
		start = System.currentTimeMillis();
		//buf.array();
		System.out.println("buf to array time taken "
						+(System.currentTimeMillis() - start));
		
		
		start = System.currentTimeMillis();
		//resultJSON = new JSONArray(jsonString);
		offset = 0;
		for( int i=0; i<numGuids; i++ )
		{
			byte[] guidBytes = new byte[20];
			System.arraycopy(byteArray, offset, guidBytes, 0, guidBytes.length);
			offset = offset + guidBytes.length;
			
			
			byte[] encryptedBytes = new byte[128];
			System.arraycopy(byteArray, offset, encryptedBytes, 0, encryptedBytes.length);
			offset = offset + encryptedBytes.length;
			
			
			encryptedBytes = new byte[128];
			System.arraycopy(byteArray, offset, encryptedBytes, 0, encryptedBytes.length);
			offset = offset + encryptedBytes.length;
			
			
			encryptedBytes = new byte[128];
			System.arraycopy(byteArray, offset, encryptedBytes, 0, encryptedBytes.length);
			offset = offset + encryptedBytes.length;
		}
		System.out.println("ByteArray from time taken "
				+(System.currentTimeMillis() - start));
		// time from string
//		start = System.currentTimeMillis();
//		resultJSON = new JSONArray(jsonString);
//		System.out.println("JSON fromString time taken "
//				+(System.currentTimeMillis() - start));
	}
}