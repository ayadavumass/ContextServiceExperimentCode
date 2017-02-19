package edu.umass.cs.countycode;

import java.awt.geom.Path2D;
import java.awt.geom.Rectangle2D;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Processes the county geoJSON and convert county polygon to bounding rectangles
 * so that user GUIDs can be created there.
 * @author ayadav
 *
 */
public class CountyJSONProcessing 
{
	public static final String PROPERTIES_KEY				= "properties";
	public static final String GEOMETRY_KEY					= "geometry";
	public static final String STATEFP_KEY					= "STATEFP";
	public static final String COUNTYFP_KEY					= "COUNTYFP";
	public static final String TYPE_KEY						= "type";
	public static final String COORD_KEY					= "coordinates";
	public static final String NAME_KEY						= "NAMELSAD";
	
	public static String COUNTY_DATA_FILE 
				= "/home/ayadav/Documents/Data/CountyPopulation/tl_2015_us_county/county.json";
	
	public static String PROCESSED_COUNTY_DATA = "processedCountyData.csv";
	
	
	private static void processCountyJSON()
	{
		BufferedReader br = null;
		BufferedWriter bw = null;
		
		try
		{
			String sCurrentLine;
			br = new BufferedReader(new FileReader(COUNTY_DATA_FILE));
			bw = new BufferedWriter(new FileWriter(PROCESSED_COUNTY_DATA));
			
			
			// skipping first line
			br.readLine();
			
			while ((sCurrentLine = br.readLine()) != null) 
			{
				if(sCurrentLine.startsWith("{"))
				{
					try
					{
						JSONObject countyJSON   = new JSONObject(sCurrentLine);
						JSONObject propertyJSON = countyJSON.getJSONObject(PROPERTIES_KEY);
						JSONObject geometryJSON = countyJSON.getJSONObject(GEOMETRY_KEY);
						
						String stateFP          = propertyJSON.getString(STATEFP_KEY);
						String countyFP         = propertyJSON.getString(COUNTYFP_KEY);
						String countyName       = propertyJSON.getString(NAME_KEY);
						String polygonType      = geometryJSON.getString(TYPE_KEY);
						JSONArray polygonCoord  = geometryJSON.getJSONArray(COORD_KEY);
						
						
						if(polygonType.equals("Polygon"))
						{
							if(polygonCoord.length() > 1)
							{
								System.out.println("Hole polygon in counties "+polygonType
										+" num polygons "+polygonCoord.length()
										+ " stateFP "+stateFP
										+ " countyFP "+countyFP
										+" countyName "+countyName);
							}
							CountyBounds countyBound 
									= computeBoundingPolygon(polygonCoord);
							
							String countyInfo = stateFP+","+countyFP+","+countyName
									+","+countyBound.lowerLatitude+","+countyBound.lowerLongitude
									+","+countyBound.upperLatitude+","+countyBound.upperLongitude+"\n";
							
							bw.write(countyInfo);
						}
						else
						{	
							System.out.println("Non polygon in counties "+polygonType
										+" num polygons "+polygonCoord.length()
										+ " stateFP "+stateFP
										+ " countyFP "+countyFP
										+" countyName "+countyName);
							
							
							// processing just first polygon
							CountyBounds countyBound 
								= computeBoundingPolygon(polygonCoord.getJSONArray(0));
					
							String countyInfo = stateFP+","+countyFP+","+countyName
									+","+countyBound.lowerLatitude+","+countyBound.lowerLongitude
									+","+countyBound.upperLatitude+","+countyBound.upperLongitude+"\n";
					
							bw.write(countyInfo);
						}
						
					} catch (JSONException e) 
					{
						e.printStackTrace();
					}
				}
			}
			
		} catch (IOException e) 
		{
			e.printStackTrace();
		} finally 
		{
			try 
			{
				if (br != null)
					br.close();
				
				if( bw != null)
					bw.close();
				
			} catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
	}
	
	
	private static CountyBounds computeBoundingPolygon(JSONArray polygonCoord) 
					throws JSONException
	{	
		
		JSONArray innerArray = polygonCoord.getJSONArray(0);
		
		JSONArray coordArray = innerArray.getJSONArray(0);
		double longitude = coordArray.getDouble(0);
		double latitude = coordArray.getDouble(1);
		
		Path2D geoJSONPolygon = new Path2D.Double();
		geoJSONPolygon.moveTo( latitude, longitude );
		
		for( int i=1; i<innerArray.length(); i++ )
		{
			coordArray = innerArray.getJSONArray(i);
			longitude = coordArray.getDouble(0);
			latitude = coordArray.getDouble(1);
			
			geoJSONPolygon.lineTo( latitude, longitude );
		}
		
		geoJSONPolygon.closePath();
		Rectangle2D boundingRect = geoJSONPolygon.getBounds2D();
		
		double minLat  = boundingRect.getMinX();
		double maxLat  = boundingRect.getMaxX();
		double minLong = boundingRect.getMinY();
		double maxLong = boundingRect.getMaxY();
		
		CountyBounds countyBounds = new CountyBounds();
		countyBounds.lowerLatitude = minLat;
		countyBounds.upperLatitude = maxLat;
		countyBounds.lowerLongitude = minLong;
		countyBounds.upperLongitude = maxLong;
		
		return countyBounds;
	}
	
	
	public static void main(String[] args)
	{
		processCountyJSON();
	}
	
	
	private static class CountyBounds
	{
		double lowerLatitude;
		double upperLatitude;
		double lowerLongitude;
		double upperLongitude;
	}
}