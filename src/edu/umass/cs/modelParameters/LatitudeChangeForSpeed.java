package edu.umass.cs.modelParameters;

import edu.umass.cs.acs.geodesy.GeodeticCalculator;
import edu.umass.cs.acs.geodesy.GlobalCoordinate;
import edu.umass.cs.weatherExpClient.WeatherAndMobilityBoth;

public class LatitudeChangeForSpeed 
{
	public static void main(String[] args)
	{
		double latitude = 32;
		double longitude = -97;
		GlobalCoordinate gcord 
			= new GlobalCoordinate(latitude, longitude);
		
		double movementAngle = 45;
		double distanceInMeters 
			= (WeatherAndMobilityBoth.SPEED_DRIVING * 1.6 * 1000.0)*(WeatherAndMobilityBoth.granularityOfGeolocationUpdate/(1000.0*3600));
		
		GlobalCoordinate endCoord = GeodeticCalculator.calculateEndingGlobalCoordinates
			(gcord, movementAngle, distanceInMeters);
		
		System.out.println("new lat "+endCoord.getLatitude()+" new long "+endCoord.getLongitude());
		double latDiff = Math.abs(endCoord.getLatitude()-latitude);
		double longDiff = Math.abs(endCoord.getLongitude()-longitude);
		calculateNumNodesForUpdateBasic(Math.max(latDiff, longDiff));
	}
	
	public static void calculateNumNodesForUpdateBasic(double latChange)
	{
		for(int i=1;i<=36;i++)
		{
			double numNodes = i;
			double numPartitions = Math.sqrt(numNodes);
			double perPartLat 
			=  (WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN)/numPartitions;
			perPartLat = perPartLat/2;
			double numTimesSingleUpdate = perPartLat/latChange;
			double expectedNumNodes = 2.0/(numTimesSingleUpdate+1) + numTimesSingleUpdate/(numTimesSingleUpdate+1);
			System.out.println("numNodes "+numNodes+" expected update node touch "+expectedNumNodes);
		}
	}
}