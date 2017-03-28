package edu.umass.cs.largescalecasestudy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import edu.umass.cs.contextservice.utils.Utils;

public class BulkLoadingFileGenerator
{
	public static final double MIN_US_LAT						= 22.0;
	public static final double MAX_US_LAT						= 48.0;
	
	public static final double MIN_US_LONG						= -125.0;
	public static final double MAX_US_LONG						= -66.0;
	
	public static final String GUID_PREFIX 						= "GUID_PREFIX";
	
	public static final String COUNTY_INFO_FILE 	
																= "/proj/MobilityFirst/ayadavDir/contextServiceScripts/countyData.csv";
	
	//public static final String COUNTY_INFO_FILE 	
	//															= "/home/ayadav/Documents/Data/CountyPopulation/countyData.csv";
	
	// latitude longitude key in json and attribute names in CNS
	public static final String LATITUDE_KEY						= "latitude";
	public static final String LONGITUDE_KEY					= "longitude";
	
	
	public static List<CountyNode> countyProbList;

	
	
	public static long computeSumPopulation()
	{
		BufferedReader readfile = null;
		long totalPop = 0;
		
		try
		{
			String sCurrentLine;
			readfile = new BufferedReader(new FileReader(COUNTY_INFO_FILE));
			
			while( (sCurrentLine = readfile.readLine()) != null )
			{
				String[] parsed = sCurrentLine.split(",");
				
				if( parsed.length >= 8)
					totalPop = totalPop + Long.parseLong(parsed[7]);
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				if (readfile != null)
					readfile.close();				
			} 
			catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
		return totalPop;
	}
	
	
	private static void computeCountyPopDistribution(long totalPop)
	{
		BufferedReader readfile = null;
		
		try
		{
			String sCurrentLine;
			readfile = new BufferedReader(new FileReader(COUNTY_INFO_FILE));
			
			double lastUpperBound = 0.0;
			
			while( (sCurrentLine = readfile.readLine()) != null )
			{
				String[] parsed = sCurrentLine.split(",");
				
				if( !(parsed.length >= 8) )
					continue;
					
				
				int statefp = Integer.parseInt(parsed[0]);
				int countyfp = Integer.parseInt(parsed[1]);
				String countyname = parsed[2];
				double minLat = Double.parseDouble(parsed[3]);
				double minLong = Double.parseDouble(parsed[4]);
				double maxLat = Double.parseDouble(parsed[5]);
				double maxLong = Double.parseDouble(parsed[6]);
				long countypop = Long.parseLong(parsed[7]);
				
				double prob = (countypop*1.0)/(totalPop*1.0);
				
				CountyNode countynode = new CountyNode();
				countynode.statefp = statefp;
				countynode.countyfp = countyfp;
				countynode.countyname = countyname;
				countynode.minLat = minLat;
				countynode.minLong = minLong;
				countynode.maxLat = maxLat;
				countynode.maxLong = maxLong;
				countynode.population = countypop;
				
				
				countynode.lowerProbBound = lastUpperBound;
				countynode.upperProbBound = countynode.lowerProbBound + prob;
				
				lastUpperBound = countynode.upperProbBound;
				
				countyProbList.add(countynode);
			}
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		} 
		finally 
		{
			try
			{
				if (readfile != null)
					readfile.close();				
			} catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
	}
	
	/**
	 * Implements the binary search of county in which the given random
	 * variable lies. The code implements choosing a county to add a person based
	 * on counties population. Binary search is needed because we have 3000 counties
	 * Linear search each time will be expensive.
	 * @return
	 */
	public static CountyNode binarySearchOfCounty(double randomVal)
	{
		int lowerBound = 0;
		int upperBound = countyProbList.size() -1;
		int mid = (lowerBound+upperBound)/2;
		
		boolean cont = true;
		CountyNode retNode = null;
		do
		{
			CountyNode countynode = countyProbList.get(mid);
			if( (randomVal >= countynode.lowerProbBound) && 
						(randomVal <countynode.upperProbBound) )
			{
				retNode = countynode;
				break;
			}
			else
			{
				if( randomVal < countynode.lowerProbBound )
				{
					upperBound = mid-1;
					assert(upperBound >=0);
					mid = (lowerBound+upperBound)/2;
				}
				else if( randomVal >= countynode.upperProbBound )
				{
					lowerBound = mid+1;
					assert(lowerBound < countyProbList.size());
					mid = (lowerBound+upperBound)/2;
				}
				else
				{
					assert(false);
				}
			}
		} 
		while(cont);
		assert(retNode != null);
		return retNode;
	}
	
	
	private static void computeGlobalLatLongBounds()
	{
		double minLat    = 1000;
		double minLong   = 1000;
		double maxLat    = -1000;
		double maxLong   = -1000;
		
		for(int i=0; i<countyProbList.size(); i++)
		{
			CountyNode countynode = countyProbList.get(i);
			
			if(  countynode.minLat < minLat )
			{
				minLat = countynode.minLat;
			}
			
			
			if( countynode.minLong < minLong )
			{
				minLong = countynode.minLong;
			}
			
			if( countynode.maxLat > maxLat )
			{
				maxLat = countynode.maxLat;
			}
			
			
			if( countynode.maxLong > maxLong )
			{
				maxLong = countynode.maxLong;
			}
		}	
		System.out.println("minLat="+minLat+", minLong="+minLong
					+", maxLat="+maxLat+", maxLong="+maxLong);
	}
	
	
	/*public static String getSHA1(String stringToHash)
	{
		MessageDigest md = null;
		try
		{
			md = MessageDigest.getInstance("SHA-256");
		} 
		catch (NoSuchAlgorithmException e)
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
	}*/
	
	public static String getOrderedHash(long guidNum)
	{
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
	    buffer.putLong(guidNum);
	    String hexArray = Utils.byteArrayToHex(buffer.array());
	    
	    for(int i=0; i<(40-hexArray.length()); i++)
	    {
	    	hexArray = "0"+hexArray ;
	    }
		return hexArray;
	}
	
	
	private static void generateGuidEntry(long guidNum, BufferedWriter bw, Random rand) throws Exception
	{
		double randnum = rand.nextDouble();
		
		CountyNode countynode = binarySearchOfCounty(randnum);
		
		double homeLat =  countynode.minLat + 
					(countynode.maxLat - countynode.minLat) * rand.nextDouble();
		
		double homeLong = countynode.minLong + 
					(countynode.maxLong - countynode.minLong) * rand.nextDouble();
		
		String userGUID = getOrderedHash(guidNum);
		
		
		String str = userGUID+","+homeLat+","+homeLong+"\n";
		bw.write(str);
	}
	
	
	public static void main(String[] args) throws Exception
	{
		long numusers 	= Long.parseLong(args[0]);
		
		Random rand = new Random(100);
		
		countyProbList = new LinkedList<CountyNode>();
		
		
		long totalPop  = computeSumPopulation();
		
		computeCountyPopDistribution(totalPop);
		
		computeGlobalLatLongBounds();
		
		
		BufferedWriter bw = null;
		
		try
		{
			bw = new BufferedWriter(new FileWriter("guidInfoFile.txt"));
			String str = "# guid, latitude, longitude\n";
			bw.write(str);
			
			long currnum = 0;
			
			while(currnum < numusers)
			{
				generateGuidEntry(currnum, bw, rand);
				currnum++;
			}
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
		}
		finally
		{
			if(bw != null)
			{
				bw.close();
			}
		}
		
		System.exit(0);
	}
}