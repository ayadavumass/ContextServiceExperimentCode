package edu.umass.cs.utils;


public class NumCores
{
	public static void main(String[] args)
	{
		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("cores "+cores);
	}
}