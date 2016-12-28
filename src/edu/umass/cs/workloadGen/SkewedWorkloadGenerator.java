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
	
	public static final int NUM_GUIDs					= 100;
	
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
//		String searchQuery
//			= "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE ";
		
		String searchQuery = "";
		
		HashMap<String, Boolean> distinctAttrMap 
			= pickDistinctAttrs( NUM_QUERY_ATTRs, NUM_ATTRs );
		
		Iterator<String> attrIter = distinctAttrMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			
			double attrMin = convertGuassianIntoValInRange(randGen.nextGaussian());
			
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
		String userGUID = "";
		
		int guidNum = randGen.nextInt(NUM_GUIDs);
		userGUID = getSHA1(GUID_PREFIX+guidNum);
		
		JSONObject attrValJSON = guidAttrMap.get(userGUID);
		
		String uAttrName = pickAttrUsingGaussian();
		
		double uAttrVal = convertGuassianIntoValInRange(randGen.nextGaussian());
		
		// GUID and new attrName and value.
		String str = userGUID+","+uAttrName+","+uAttrVal;
		
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
				double attrVal = convertGuassianIntoValInRange(randGen.nextGaussian());
				
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
	
	
	private HashMap<String, Boolean> pickDistinctAttrs( int numAttrsToPick, 
			int totalAttrs )
	{
		HashMap<String, Boolean> hashMap = new HashMap<String, Boolean>();
		int currAttrNum = 0;
		
		while(hashMap.size() != numAttrsToPick)
		{
			if( NUM_ATTRs == NUM_QUERY_ATTRs )
			{
				String attrName = "attr"+currAttrNum;
				hashMap.put(attrName, true);
				currAttrNum++;
			}
			else
			{
				String attrName = pickAttrUsingGaussian();
				//currAttrNum = randGen.nextInt(NUM_ATTRs);
				//String attrName = "attr"+currAttrNum;
				hashMap.put(attrName, true);
			}
		}
		return hashMap;
	}
	
	
	private String pickAttrUsingGaussian()
	{
		// between -2 and 2.
		double gaussianRandVal = randGen.nextGaussian();
		double midPoint = NUM_ATTRs/2.0;	
		
		if( gaussianRandVal >= 0 )
		{
			if( gaussianRandVal > 2 )
			{
				gaussianRandVal = 2;
			}
			
			int attrNum = (int) Math.ceil(midPoint + (gaussianRandVal*midPoint)/2.0);
			
			// because attr are numbered from 0 to NUM_ATTRs-1
			if( attrNum >= 1)
			{
				attrNum = attrNum -1 ;
			}
			
			String attrName = "attr"+attrNum;
			return attrName;
		}
		else
		{
			gaussianRandVal = -gaussianRandVal;
			
			if( gaussianRandVal > 2 )
			{
				gaussianRandVal = 2;
			}
			
			int attrNum = (int) Math.ceil((gaussianRandVal*midPoint)/2.0);
			
			// because attr are numbered from 0 to NUM_ATTRs-1
			if( attrNum >= 1)
			{
				attrNum = attrNum -1 ;	
			}
			
			String attrName = "attr"+attrNum;
			
			return attrName;
		}
	}
	
	
	private double convertGuassianIntoValInRange(double guassionRandVal)
	{
		double valInRange = 0;
		double midpoint = ((ATTR_MAX+ATTR_MIN)/2.0);
		
		if( guassionRandVal >= 0 )
		{
			if( guassionRandVal > 2 )
			{
				guassionRandVal = 2;
			}
			
			valInRange = midpoint + (guassionRandVal*midpoint)/2.0;
			return valInRange;
		}
		else
		{
			guassionRandVal = -guassionRandVal;
			
			if( guassionRandVal > 2 )
			{
				guassionRandVal = 2;
			}
			
			valInRange = (guassionRandVal*midpoint)/2.0;
			return valInRange;
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