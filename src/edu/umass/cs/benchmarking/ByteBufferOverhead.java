package edu.umass.cs.benchmarking;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * ByteBuffer and ByteArray has 5ms overhead difference.
 * ByteBuffer takes 33ms, ByteArray takes 28ms.
 * @author adipc
 */
public class ByteBufferOverhead 
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
		ByteBuffer buf = ByteBuffer.allocate(SIZE_OF_EACH_ELEMENT*numGuids);
		for(int i=0; i<numGuids; i++)
		{
			byte[] guidBytes = new byte[20];
			rand.nextBytes(guidBytes);
			//String guidString = Utils.bytArrayToHex(guidBytes);
			buf.put(guidBytes);
			
			
			byte[] encryptedBytes = new byte[128];
			rand.nextBytes(encryptedBytes);
			buf.put(encryptedBytes);
			
			
			encryptedBytes = new byte[128];
			rand.nextBytes(encryptedBytes);
			buf.put(encryptedBytes);
			
			
			encryptedBytes = new byte[128];
			rand.nextBytes(encryptedBytes);
			buf.put(encryptedBytes);	
		}
		buf.flip();
		System.out.println("time taken "+(System.currentTimeMillis() - start));
		
		// time to convert to json tostring
		start = System.currentTimeMillis();
		byte[] byteArr = buf.array();
		System.out.println("buf to array time taken "
						+(System.currentTimeMillis() - start));
		
		// time from string
		
		start = System.currentTimeMillis();
		//resultJSON = new JSONArray(jsonString);
		ByteBuffer readBuf = ByteBuffer.wrap(byteArr);
		for( int i=0; i<numGuids; i++ )
		{
			byte[] guidBytes = new byte[20];
			readBuf.get(guidBytes);
			
			byte[] encryptedBytes = new byte[128];
			readBuf.get(encryptedBytes);
			
			encryptedBytes = new byte[128];
			readBuf.get(encryptedBytes);
			
			encryptedBytes = new byte[128];
			readBuf.get(encryptedBytes);
		}
		System.out.println("ByteBuffer from time taken "
				+(System.currentTimeMillis() - start));
	}
}