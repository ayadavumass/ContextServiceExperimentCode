/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.weatherExpClient;

import edu.umass.cs.acs.geodesy.GlobalCoordinate;

import java.util.List;

import org.json.JSONException;

/**
 * Static helper methods for accessing Mongo tables. 
 * @author westy
 */
public class GnsDatabase 
{
  //  "{\"~geoLocationCurrent\": {"
  //          + "      $geoWithin: {"
  //          + "         $geometry: {"
  //          + "            type: \"Polygon\","
  //          + "            coordinates: [ <coordinates> ]\n"
  //          + "         }"
  //          + "      }"
  //          + "   }"
  //          + "}";
  public static String buildQuery(String locationField, List<GlobalCoordinate> coordinates) throws JSONException {
    return "~" + locationField + ":{"
            + "$geoWithin:{"
            + "$geometry:"
            + GeoJSON.createGeoJSONPolygon(coordinates).toString()
            + "}"
            + "}";
  }

  public static String buildQuery(String locationField, List<GlobalCoordinate> coordinates, String ageField, int age1, int age2) throws JSONException {
    return "$and: ["
            + "{~" + locationField + ":{"
            + "$geoWithin:{"
            + "$geometry:"
            + GeoJSON.createGeoJSONPolygon(coordinates).toString()
            + "}}},"
            + "{~" + ageField + ":{$gt:" + age1 + ", $lt:" + age2 + "}}"
            + "]";
  }

  public static String buildOrQuery(String... clauses) {
    StringBuilder result = new StringBuilder();
    String prefix = "";
    result.append("$or: [");
    for (String clause : clauses) {
      result.append(prefix);
      result.append("{");
      result.append(clause);
      result.append("}");
      prefix = ",";
    }
    result.append("]");
    return result.toString();
  }
  
  public static String buildLocationsQuery(List<GlobalCoordinate> coordinates) throws JSONException {
    return GnsDatabase.buildOrQuery(
            GnsDatabase.buildQuery("geoLocationCurrent", coordinates),
            //GnsDatabase.buildQuery("geoLocations.Home", coordinates),
            //GnsDatabase.buildQuery("geoLocations.Work", coordinates),
            GnsDatabase.buildQuery("customLocations.location.coordinates", coordinates)
    );
  }
  
  public static String buildContextServiceQuery(String latAttrName, String longAttrName, 
		  List<GlobalCoordinate> coordinates) throws JSONException
  {
	  String geoJSONString = GeoJSON.createGeoJSONPolygon(coordinates).toString();
	  String query = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap("+latAttrName+", "+
			  longAttrName+", "+geoJSONString+")";
	  return query;
  }
}