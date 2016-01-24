package edu.umass.cs.nomadlogProcessing;
import java.util.Arrays;

/**
 * contains methods to return mean, median, min, max, 5% 95% of an array
 * @author adipc
 *
 */
public class StatClass 
{
	private final double mean;
	private final double median;
	private final double min;
	private final double max;
	private final double fivePerc;
	private final double nineFivePerc;
	
	public StatClass(Double[] valueArray)
	{
		System.out.println("valueArray size "+valueArray.length+" "+valueArray[0]);
		Arrays.sort(valueArray);
		
		mean = getMean(valueArray);
		median = valueArray[valueArray.length/2];
		min = valueArray[0];
		max = valueArray[valueArray.length - 1];
		fivePerc = valueArray[(int)(0.05*valueArray.length)];
		nineFivePerc = valueArray[(int)(0.95*valueArray.length)];
	}
	
	private double getMean(Double[] valueArray)
	{
		double sum = 0;
		for (int i=0;i<valueArray.length;i++)
		{
			sum += valueArray[i];
		}
		return sum/valueArray.length;
	}
	
	public double getMean()
	{
		return this.mean;
	}
	
	public double getMedian()
	{
		return this.median;
	}

	public double getMin()
	{
		return this.min;
	}
	
	public double getMax()
	{
		return this.max;
	}
	
	public double get5Perc()
	{
		return this.fivePerc;
	}
	
	public double get95Perc()
	{
		return this.nineFivePerc;
	}
}