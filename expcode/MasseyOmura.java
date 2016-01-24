package edu.umass.cs.expcode;
import java.math.BigInteger;
import java.util.Random;

/**
 * 
 * @author jwallrab
 * @contact me@jrwallrabenstein.com
 * @description An implementation of the Massey-Omura Cryptosystem
 */

public class MasseyOmura
{	
	/**
	 * Number of times the algorithm will attempt to
	 * generate valid parameters.
	 */
	private final int attempts = 100;

	/**
	 * The human readable name of the cryptosystem
	 */
	private String title = "MasseyOmura";

	/**
	 * Returns the name of the cryptosystem
	 */
	public String getTitle()
	{
		return title;
	}

	/**
	 * The parameters to the cryptosystem
	 */
	private BigInteger p, e, d, pMinusOne;

	/**
	 * The number of bits used for the prime and keys
	 */
	private int bitLength = 2048;

	/**
	 * Generic constructor; used when the prime modulus is
	 * to be chosen by a different party.
	 */
	public MasseyOmura()
	{
	}
	
	/**
	 * Constructor for the party choosing the number of bits
	 * used for the parameters, and the certainty that the
	 * generated number p is in fact a prime.
	 * @param bitLengthVal
	 * @param certainty
	 */
	public MasseyOmura(int bitLengthVal, int certainty)
	{
		generateKeys(bitLengthVal, certainty);
	}
	

	/**
	 * This method generates a large prime p, that is prime
	 * with the specified certainty. The encryption key is
	 * chosen uniformly at random, and the decryption key
	 * is chosen as its multiplicative inverse modulo (p-1).
	 * @param bitLengthVal
	 * @param certainty
	 */
	public void generateKeys(int bitLengthVal, int certainty)
	{
		bitLength = bitLengthVal;

		/* Ensure that p is prime, e < p, and d = e^(-1) mod (p-1) exists */
		int ctr = 0;
		while(ctr < attempts){

			/* Choose p and store (p-1) */
			p = new BigInteger(bitLength, certainty, new Random(0));
			pMinusOne = p.subtract(BigInteger.ONE);
			
			/* Choose e uniformly at random from {0,1}^bitLength */
			e = new BigInteger(bitLength, new Random());

			/* Ensure that e < p */
			while(e.compareTo(p) == 1)
				e = new BigInteger(bitLength, new Random());
			
			/* Attempt to find the multiplicative inverse d of e (mod p-1) */
			try{
				d = e.modInverse(pMinusOne);
				return;
			}catch(Exception e){
				ctr++;
			}
		}
		
		/* Parameter generation has failed - may be replaced with an exception */
		ContextServiceLogger.getLogger().fine("Failed to find suitable parameters");
		System.exit(0);
	}

	/**
	 * Generates encryption and decryption keys from a given prime modulus.
	 * @param bitLengthVal
	 * @param certainty
	 * @param prime
	 */
	public void generateKeys(BigInteger prime)
	{
		p = prime;
		pMinusOne = p.subtract(BigInteger.ONE);

		int ctr = 0;
		while(ctr < 100){

			/* Choose e uniformly at random from {0,1}^bitLength */
			e = new BigInteger(bitLength, new Random());

			/* Ensure that e < p */
			while(e.compareTo(p) == 1)
				e = new BigInteger(bitLength, new Random());
			
			/* Attempt to find the multiplicative inverse d of e (mod p-1) */
			try{
				d = e.modInverse(pMinusOne);
				return;
			}catch(Exception e){
				ctr++;
			}
		}
		
		/* Parameter generation has failed - may be replaced with an exception */
		ContextServiceLogger.getLogger().fine("Failed to find suitable parameters");
		System.exit(0);
	}

	/**
	 * Encrypts the plaintext message m as:
	 * c = m^e (mod p)
	 */
	public BigInteger encrypt(BigInteger m)
	{
		return m.modPow(e, p);
	}

	/**
	 * Decrypts the ciphertext message c as:
	 * m = c^d (mod p)
	 */
	public BigInteger decrypt(BigInteger c)
	{
		return c.modPow(d, p);
	}

	/**
	 * Returns the shared prime modulus p
	 * @return shared prime p
	 */
	public BigInteger getSharedModulus()
	{
		return new BigInteger(p.toString());
	}

	/**
	 * Calculates the modular multiplicative inverse
	 * of param modulo the shared prime modulus p
	 * @param param
	 * @return
	 */
	public BigInteger getModInverse(BigInteger param)
	{
		return param.modInverse(p);
	}
	
	public static BigInteger toBigInteger(String foo)
	{
	    return new BigInteger(foo.getBytes());
	}

	public static String fromBigInteger(BigInteger bar)
	{
	    return new String(bar.toByteArray());
	}
	
	public static void main(String[] args)
	{
		MasseyOmura mo1 = new MasseyOmura(2048, 1);
		MasseyOmura mo2 = new MasseyOmura(2048, 1);
		
		String message = "This is a very secret message";
		BigInteger enc1 = mo1.encrypt(toBigInteger(message));
		BigInteger enc2 = mo2.encrypt(enc1);
		BigInteger enc3 = mo1.decrypt(enc2);
		BigInteger enc4 = mo2.decrypt(enc3);
		ContextServiceLogger.getLogger().fine(fromBigInteger(enc4));
		
	}
}