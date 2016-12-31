package edu.umass.cs.workloadGen;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;


public class SkewedWorkloadGenerator
{
	public static final int NUM_SEARCH				  	= 1000;
	public static final int NUM_UPDATES				  	= 1000;
	
	public static final int NUM_ATTRs				  	= 20;
	
	public static final int NUM_QUERY_ATTRs		      	= 4;
	
	public static final double ATTR_MIN				  	= 1.0;
	public static final double ATTR_MAX				  	= 1500.0;
	
	// 0.3 is 30% of predicate length
	public static final double PREDICATE_LENGTH_RATIO   = 0.3;
	
	public static final String SEARCH_FILE_NAME			= "searchFile.txt";
	public static final String UPDATE_FILE_NAME			= "updateFile.txt";
	
	public static final String GUID_PREFIX				= "GUID_PREFIX";
	
	public static final int NUM_GUIDs					= 10000;
	
	// that is range from 650-850 has 70% prob
	public static final double RANGE_STD_DEV			= 150.0;

	// that is attr8, attr9 attr10   has 70% prob
	public static final double ATTR_STD_DEV				= 2.0;
	
	
	//public static final double GUID_STD_DEV				= 1500.0;
	
	
	
	private final Random randGen;
	
	private final HashMap<String, JSONObject> guidAttrMap;
	
	
	public SkewedWorkloadGenerator()
	{
		randGen = new Random();
		guidAttrMap = new HashMap<String, JSONObject>();
		
		createGUIDRecords();
	}
	
	
	public void generateAndWriteSearchQueriesToFile()
	{
		BufferedWriter bw 	= null;
		FileWriter fw 		= null;
		
		try
		{
			fw = new FileWriter(SEARCH_FILE_NAME);
			bw = new BufferedWriter(fw);
				
			for(int i=0; i<NUM_SEARCH ; i++)
			{
				String searchQ = createSearchQuery();
				bw.write(searchQ +"\n");
			}
			
		} catch (IOException e) 
		{
			e.printStackTrace();
		}
		finally
		{
			try 
			{
				if (bw != null)
					bw.close();
					
				if (fw != null)
					fw.close();
			} catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
	}
	
	
	public void generateAndWriteUpdateRequestsToFile()
	{	
		BufferedWriter bw 	= null;
		FileWriter fw 		= null;
		
		try
		{
			fw = new FileWriter(UPDATE_FILE_NAME);
			bw = new BufferedWriter(fw);
				
			for(int i=0; i<NUM_UPDATES ; i++)
			{
				String str = createUpdateMessage();
				bw.write(str+"\n");
			}
			
		} catch (IOException e) 
		{
			e.printStackTrace();
		}
		finally
		{
			try 
			{
				if (bw != null)
					bw.close();
					
				if (fw != null)
					fw.close();
			} catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
	}
	
	
	private String createSearchQuery()
	{	
		String searchQuery = "";
		
		HashMap<String, Boolean> distinctAttrMap 
			= pickDistinctAttrs( NUM_QUERY_ATTRs, randGen );
		
		Iterator<String> attrIter = distinctAttrMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			
			double attrMin = convertGuassianIntoValInRange(randGen);
			
			// querying 10 % of domain
			double predLength 
				= (PREDICATE_LENGTH_RATIO*(ATTR_MAX - ATTR_MIN));
			
			double attrMax = Math.min(attrMin + predLength, ATTR_MAX);
			
			// last so no AND
			if( !attrIter.hasNext() )
			{
				searchQuery = searchQuery + " "+attrName+" >= "+attrMin+" AND "+attrName
						+" <= "+attrMax;
			}
			else
			{
				searchQuery = searchQuery + " "+attrName+" >= "+attrMin+" AND "+attrName
					+" <= "+attrMax+" AND ";
			}
		}
		return searchQuery;
	}
	
	
	private String createUpdateMessage()
	{
		//String userGUID = pickGUIDUsingGaussian(randGen);
		String userGUID = getSHA1(GUID_PREFIX+randGen.nextInt(NUM_GUIDs));
		
		JSONObject attrValJSON = guidAttrMap.get(userGUID);
		
		String uAttrName = pickAttrUsingGaussian(randGen);
		
		double uAttrVal = convertGuassianIntoValInRange(randGen);
		
		// GUID and new attrName and value.
		String str = userGUID+","+uAttrName+","+uAttrVal;
		
		@SuppressWarnings("unchecked")
		Iterator<String> attrIter = attrValJSON.keys();
		
		// old values for this GUID.
		while( attrIter.hasNext() )
		{
			try 
			{
				String currAttrName = attrIter.next();
				String currAttrVal = attrValJSON.getString(currAttrName);
				
				str = str +","+currAttrName+","+currAttrVal;
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		
		// update the values in the GUID json.
		try
		{
			attrValJSON.put(uAttrName, uAttrVal);
		} 
		catch (JSONException e) 
		{
			e.printStackTrace();
		}	
		return str;
	}
	
	
	private void createGUIDRecords()
	{
		for( int i=0; i<NUM_GUIDs; i++ )
		{
			String userGUID = "";
			int guidNum = i;
			userGUID = getSHA1(GUID_PREFIX+guidNum);
			
			JSONObject attrValJSON = new JSONObject();
			
			for(int j=0; j<NUM_ATTRs; j++)
			{
				String attrName = "attr"+j;
				double attrVal = convertGuassianIntoValInRange(randGen);
				
				try 
				{
					attrValJSON.put(attrName, attrVal);
				} catch (JSONException e) 
				{
					e.printStackTrace();
				}
			}
			
			guidAttrMap.put(userGUID, attrValJSON);			
		}
	}
	
	
	private HashMap<String, Boolean> pickDistinctAttrs( int numAttrsToPick, Random randGen)
	{
		HashMap<String, Boolean> hashMap = new HashMap<String, Boolean>();
		
		while(hashMap.size() != numAttrsToPick)
		//for(int i=0; i<numAttrsToPick; i++)
		{
			String attrName = pickAttrUsingGaussian(randGen);
			hashMap.put(attrName, true);
		}
		return hashMap;
	}
	
	
	private String pickAttrUsingGaussian(Random randGen)
	{
		while(true)
		{
			double gaussianRandVal = randGen.nextGaussian();
			
			int midpointAttrNum = NUM_ATTRs/2 -1;
					
			if( gaussianRandVal >= 0 )
			{	
				int attrNum =  midpointAttrNum+(int) Math.round(gaussianRandVal*ATTR_STD_DEV);
				
				if(attrNum >= 0 && attrNum < NUM_ATTRs)
				{
					String attrName = "attr"+attrNum;
					return attrName;
				}
				else
				{
					System.out.println("Out of range generation attr"+attrNum);
				}
			}
			else
			{
				gaussianRandVal = -gaussianRandVal;
				int attrNum =  midpointAttrNum-(int) Math.round(gaussianRandVal*ATTR_STD_DEV);
				
				if(attrNum >= 0 && attrNum < NUM_ATTRs)
				{
					String attrName = "attr"+attrNum;
					return attrName;
				}
				else
				{
					System.out.println("Out of range generation attr"+attrNum);
				}
			}
		}
	}
	
	
	/*private String pickGUIDUsingGaussian(Random randGen)
	{
		while(true)
		{
			double gaussianRandVal = randGen.nextGaussian();
			
			int midpointGuidNum = NUM_GUIDs/2 -1;
					
			if( gaussianRandVal >= 0 )
			{	
				int guidNum =  midpointGuidNum+(int) Math.round(gaussianRandVal*GUID_STD_DEV);
				
				if(guidNum >= 0 && guidNum < NUM_GUIDs)
				{
					String userGUID = getSHA1(GUID_PREFIX+guidNum);
					return userGUID;
				}
				else
				{
					System.out.println("Out of range generation attr"+guidNum);
				}
			}
			else
			{
				gaussianRandVal = -gaussianRandVal;
				int guidNum =  midpointGuidNum-(int) Math.round(gaussianRandVal*GUID_STD_DEV);
				
				if(guidNum >= 0 && guidNum < NUM_GUIDs)
				{
					String userGUID = getSHA1(GUID_PREFIX+guidNum);
					return userGUID;
				}
				else
				{
					System.out.println("Out of range generation attr"+guidNum);
				}
			}
		}
	}*/
	
	
	public static double convertGuassianIntoValInRange(Random randGen)
	{
		double valInRange = 0;
		double midpoint = ((ATTR_MAX+ATTR_MIN)/2.0);
		
		while(true)
		{
			double guassionRandVal = randGen.nextGaussian();
			
			if( guassionRandVal >= 0 )
			{
				valInRange = midpoint + (guassionRandVal*RANGE_STD_DEV);
				
				if(valInRange >= ATTR_MIN && valInRange <= ATTR_MAX)
				{
					return valInRange;
				}
				else
				{
					System.out.println("Out of range generation val"+valInRange);
				}
			}
			else
			{
				guassionRandVal = -guassionRandVal;
				valInRange = midpoint - (guassionRandVal*RANGE_STD_DEV);
				
				if(valInRange >= ATTR_MIN && valInRange <= ATTR_MAX)
				{
					return valInRange;
				}
				else
				{
					System.out.println("Out of range generation val"+valInRange);
				}
			}
		}
	}
	
	
	private String getSHA1(String stringToHash)
	{
		MessageDigest md = null;
		try
		{
			md = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e)
		{
			e.printStackTrace();
		}
		
		md.update(stringToHash.getBytes());
		
		byte byteData[] = md.digest();
		
		//convert the byte to hex format method 1
		
		StringBuffer sb = new StringBuffer();
		
		for (int i = 0; i < byteData.length; i++) 
		{
			sb.append(Integer.toString
       				((byteData[i] & 0xff) + 0x100, 16).substring(1));
		}
		
		String returnGUID = sb.toString();
		return returnGUID.substring(0, 40);
	}
	
	
	public static void main(String[] args)
	{	
		SkewedWorkloadGenerator swg = new SkewedWorkloadGenerator();
		
		swg.generateAndWriteSearchQueriesToFile();
		swg.generateAndWriteUpdateRequestsToFile();
	}
}