package edu.umass.cs.benchmarking;

public class BitMapTesting
{
	public static void main(String args[]) 
	{
		long a = 60;	/* 60 = 0011 1100 */  
		long b = 13;	/* 13 = 0000 1101 */
		long c = 0;

	    c = a & b;       /* 12 = 0000 1100 */ 
	    System.out.println("a & b = " + c );

	    c = a | b;       /* 61 = 0011 1101 */
	    System.out.println("a | b = " + c );

	    c = a ^ b;       /* 49 = 0011 0001 */
	    System.out.println("a ^ b = " + c );

	    c = ~a;          /*-61 = 1100 0011 */
	    System.out.println("~a = " + c );

	    c = a << 2;     /* 240 = 1111 0000 */
	    System.out.println("a << 2 = " + c );

	    c = a >> 2;     /* 15 = 1111 */
	    System.out.println("a >> 2  = " + c );

	    c = a >>> 2;     /* 15 = 0000 1111 */
	    System.out.println("a >>> 2 = " + c );
	 }
}