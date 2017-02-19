package edu.umass.cs.largescalecasestudy;

public class CountyNode
{
	int statefp;
	int countyfp;
	String countyname;
	double minLat;
	double minLong;
	double maxLat;
	double maxLong;
	long population;
	double lowerProbBound;
	double upperProbBound;
	
	
	public String toString()
	{
		return "statefp="+statefp+", countyfp="+countyfp
				+", countyname="+countyname
				+", minLat="+minLat
				+", minLong="+minLong
				+", maxLat="+maxLat
				+", maxLong="+maxLong
				+", population="+population
				+", lowerProbBound="+lowerProbBound
				+", upperProbBound="+upperProbBound;		
	}
}