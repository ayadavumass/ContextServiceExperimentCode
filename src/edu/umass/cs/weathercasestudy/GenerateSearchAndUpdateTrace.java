package edu.umass.cs.weathercasestudy;

import java.awt.geom.Path2D;
import java.awt.geom.Rectangle2D;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import edu.umass.cs.acs.geodesy.GlobalCoordinate;

public class GenerateSearchAndUpdateTrace {
	
	public static final String SEARCH_FILE_NAME = "searchFile.txt";
	
	public static void generateCNSSearchQueries()
	{
		BufferedWriter searchw 	= null;
		
		try
		{
			searchw = new BufferedWriter(new FileWriter(SEARCH_FILE_NAME));
			
			WeatherDataProcessing weatherDataProcess = new WeatherDataProcessing();
			
			List<WeatherEventStorage> weatherEventList 
							= weatherDataProcess.getBuffaloAreaWeatherEvents();
			
			for(int curr=0; curr<weatherEventList.size(); curr++)
			{
				WeatherEventStorage currWeatherEvent = weatherEventList.get(curr);
				
				List<List<GlobalCoordinate>> polygonsList 
										= currWeatherEvent.getListOfPolygons();
				
				for( int i=0; i<polygonsList.size(); i++ )
				{
					List<GlobalCoordinate> polygon =  polygonsList.get(i);
					
					Path2D geoJSONPolygon = new Path2D.Double();
					GlobalCoordinate gCoord = polygon.get(0);
					geoJSONPolygon.moveTo( gCoord.getLatitude(), gCoord.getLongitude() );
					for( int j = 1; j<polygon.size(); ++j )
					{
						gCoord = polygon.get(j);
						geoJSONPolygon.lineTo( gCoord.getLatitude(), 
								gCoord.getLongitude() );
					}
					geoJSONPolygon.closePath();
					Rectangle2D boundingRect = geoJSONPolygon.getBounds2D();
					
					double minLat  = boundingRect.getMinX();
					double maxLat  = boundingRect.getMaxX();
					double minLong = boundingRect.getMinY();
					double maxLong = boundingRect.getMaxY();
					
					
					String searchQuery = SearchAndUpdateDriver.latitudeAttr+" >= "+minLat+
						" AND "+SearchAndUpdateDriver.latitudeAttr+" <= "+maxLat
						+" AND "+SearchAndUpdateDriver.longitudeAttr+" >= "+
						minLong+" AND "+SearchAndUpdateDriver.longitudeAttr+" <= "+maxLong;
					
					searchw.write(searchQuery+"\n");
				}
			}
		} catch(IOException iox)
		{
			iox.printStackTrace();
		}
		finally
		{
			try 
			{			
				if (searchw != null)
					searchw.close();
			} catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
		
	}
	
	
	public static void main( String[] args)
	{
		generateCNSSearchQueries();
	}
}