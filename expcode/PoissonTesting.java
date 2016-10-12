package edu.umass.cs.expcode;
import java.util.Random;

public class PoissonTesting
{
	
	public static void main(String[] args)
	{
		Random rand = new Random(0);
		double rate = 90;
		
		for(int i=0;i<100;i++)
		{
			double interTime = -Math.log(1-rand.nextDouble()) / rate;
			//-logf(1.0f - (float) random() / (RAND_MAX + 1)) / rateParameter;
			ContextServiceLogger.getLogger().fine("Inter arrival time "+interTime*1000);
		}
	}
}