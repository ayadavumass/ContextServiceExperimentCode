package edu.umass.cs.nomadlogProcessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import edu.umass.cs.acs.geodesy.Ellipsoid;
import edu.umass.cs.acs.geodesy.GeodeticCalculator;
import edu.umass.cs.acs.geodesy.GeodeticCurve;
import edu.umass.cs.acs.geodesy.GlobalCoordinate;


public class NomadLogProcessing
{
	// our weather trace is of 3 hours.
	// so we just want to find mobility of users within a 3 hours block.
	// this is in second interval, as nomad log time stamp is in seconds
	public static final int WeatherTraceNumMinutes		= 180*60;
	
	// entry hashmap
	public static HashMap<Integer, List<LogEntryClass>> userMobilityEntryHashMap;
	
	// inter mobility time hashmap
	public static HashMap<Integer, List<Double>> userMobilityTimeHashMap;
	
	//inter mobility distance hashmap
	public static HashMap<Integer, List<Double>> userMobilityDistanceHashMap;
	
	public static HashMap<Integer, List<Double>> windowDistanceHashMap;
	
	public static HashMap<Integer, List<Double>> windowTransitionsHashMap;
	
	public NomadLogProcessing()
	{
		userMobilityEntryHashMap 	= new HashMap<Integer, List<LogEntryClass>>();
		userMobilityTimeHashMap  	= new HashMap<Integer, List<Double>>();
		userMobilityDistanceHashMap = new HashMap<Integer, List<Double>>();
		windowDistanceHashMap 		= new HashMap<Integer, List<Double>>();
		windowTransitionsHashMap    = new HashMap<Integer, List<Double>>();
	}
	
	public void readNomadLag() throws IOException
	{
		double minBuffaloLat = 42.0;
		double maxBuffaloLat = 44.0;
		
		double minBuffaloLong = -80.0;
		double maxBuffaloLong = -78.0;
		
		
		File file = new File("buffaloTrace.txt");
		
		// if file doesn't exists, then create it
		if ( !file.exists() ) 
		{
			file.createNewFile();
		}
			
		FileWriter fw 	  = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		
		BufferedReader br = null;
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader(new FileReader
					("/home/adipc/Documents/nomadlogData/loc_seq"));
			
			
			while ((sCurrentLine = br.readLine()) != null) 
			{
				//System.out.println(sCurrentLine);
				String[] parsed = sCurrentLine.split("\\|");
//				for(int i=0;i<parsed.length;i++)
//				{
//					System.out.println("parsed string "+parsed[i]);
//				}
				int userId = Integer.parseInt(parsed[1]);
				long unixtimestamp = (long)Double.parseDouble(parsed[2]);
				double latitude = Double.parseDouble(parsed[3]);
				double longitude = Double.parseDouble(parsed[4]);
				
				if( (latitude >= minBuffaloLat) && (latitude <= maxBuffaloLat) 
						&& (longitude >= minBuffaloLong) && (longitude <= maxBuffaloLong) )
				{
					Date date = new Date(unixtimestamp*1000L); // *1000 is to convert seconds to millisecond
					String str = userId+","+unixtimestamp+","+latitude+","+longitude+","
																		+date.toGMTString()+"\n";
					bw.write(str);
				}
				else
				{
					continue;
				}
				
				LogEntryClass logEntryObj = new LogEntryClass(unixtimestamp, 
						latitude, longitude);
				
				List<LogEntryClass> userEventList = userMobilityEntryHashMap.get(userId);
				if( userEventList == null )
				{
					userEventList = new LinkedList<LogEntryClass>();
					userEventList.add(logEntryObj);
					userMobilityEntryHashMap.put(userId, userEventList);
				}
				else
				{
					userEventList.add(logEntryObj);
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
			} catch (IOException ex)
			{
				ex.printStackTrace();
			}
		}
		bw.close();
		System.out.println( "unique users "
							+userMobilityEntryHashMap.size() );
	}
	
	private void calculateInterMobilityTimeForAUser() throws IOException
	{
		File file = new File("interMobilityTimeDist.txt");
		
		// if file doesn't exists, then create it
		if ( !file.exists() )
		{
			file.createNewFile();
		}
			
		FileWriter fw 	  = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		Iterator<Integer> userIdIter = userMobilityEntryHashMap.keySet().iterator();
		
		while( userIdIter.hasNext() )
		{
			int userId = userIdIter.next();
			//List<Double> interMobilityTimeList = new LinkedList<Double>();
			List<Double> interMobilityTimeList = new ArrayList<Double>();
			
			List<LogEntryClass> userEventList = userMobilityEntryHashMap.get(userId);
			Iterator<LogEntryClass> logEntryIter = userEventList.iterator();
			
			boolean firstOne = true;
			long prev = -1;
			
			while(logEntryIter.hasNext())
			{
				LogEntryClass logEntry = logEntryIter.next();
				
				if(firstOne)
				{
					prev = logEntry.getUnixTimeStamp();
					firstOne = false;
					continue;
				}
				else
				{
					double interMobilityTime = logEntry.getUnixTimeStamp() - prev;
					prev = logEntry.getUnixTimeStamp();
					if(interMobilityTime > 0)
						interMobilityTimeList.add(interMobilityTime); 
				}
			}
			
			Double [] typeArray = new Double[1];
			typeArray[0] = new Double(0);
			System.out.println("double val "+typeArray[0]
					+" interMobilityTimeList size "+interMobilityTimeList.size() );
			//interMobilityTimeList.
			if(interMobilityTimeList.size() > 0)
			{
				StatClass statCls = new StatClass(interMobilityTimeList.toArray(typeArray));
				bw.write("userId "+userId+" mean "+statCls.getMean()+" median "+statCls.getMedian()
					+" min "+statCls.getMin()+" max "+statCls.getMax()+" 5Perc "+statCls.get5Perc()
					+" 95Perc "+statCls.get95Perc()+"\n");
				userMobilityTimeHashMap.put(userId, interMobilityTimeList);
			}
		}
		bw.close();
	}
	
	private void calculateInterMobilityDistanceForAUser() throws IOException
	{
		File file = new File("interMobilityDistanceDist.txt");
		
		// if file doesn't exists, then create it
		if ( !file.exists() )
		{
			file.createNewFile();
		}
		
		FileWriter fw 	  = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		Iterator<Integer> userIdIter = userMobilityEntryHashMap.keySet().iterator();
		
		while( userIdIter.hasNext() )
		{
			int userId = userIdIter.next();
			//List<Double> interMobilityTimeList = new LinkedList<Double>();
			List<Double> interMobilityDistList = new ArrayList<Double>();
			
			List<LogEntryClass> userEventList 	 = userMobilityEntryHashMap.get(userId);
			Iterator<LogEntryClass> logEntryIter = userEventList.iterator();
			
			boolean firstOne 	 = true;
			//double prevLatitude  = -200;
			//double prevLongitude = -200;
			GlobalCoordinate prevCoord = null;
			
			while( logEntryIter.hasNext() )
			{
				LogEntryClass logEntry = logEntryIter.next();
				
				if( firstOne )
				{
					prevCoord = new GlobalCoordinate(logEntry.getLatitude(), logEntry.getLongitude());
					firstOne  = false;
					continue;
				}
				else
				{
					double currLat  = logEntry.getLatitude();
					double currLong = logEntry.getLongitude();
					
					GlobalCoordinate currCoord 
						= new GlobalCoordinate(currLat, currLong);
					
					GeodeticCalculator geoCalc  = new GeodeticCalculator();
					
					GeodeticCurve geodeticCurve 
									= geoCalc.calculateGeodeticCurve(Ellipsoid.WGS84, prevCoord, currCoord);
					
					double distanceInMeters = geodeticCurve.getEllipsoidalDistance();
					
					prevCoord = currCoord;
					if(distanceInMeters >= 0)
						interMobilityDistList.add(distanceInMeters);
				}
			}
			
			Double [] typeArray = new Double[1];
			typeArray[0] = new Double(0);
			System.out.println("double val "+typeArray[0]
					+" interMobilityDistanceList size "+interMobilityDistList.size());
			
			if(interMobilityDistList.size() > 0)
			{
				StatClass statCls = new StatClass(interMobilityDistList.toArray(typeArray));
				bw.write("userId "+userId+" mean "+statCls.getMean()+" median "+statCls.getMedian()
				+" min "+statCls.getMin()+" max "+statCls.getMax()+" 5Perc "+statCls.get5Perc()
				+" 95Perc "+statCls.get95Perc()+"\n");
				this.userMobilityDistanceHashMap.put(userId, interMobilityDistList);
			}
		}
		bw.close();
	}
	
	public void calculateTimeCDFAcrossUsers()
	{
		BufferedReader br = null;
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader(new FileReader("/home/adipc/GNS/WeatherCaseStudy/interMobilityTimeDist.txt"));
			List<Double> meanCDF   			= new ArrayList<Double>();
			List<Double> medianCDF 			= new ArrayList<Double>();
			List<Double> fivePercCDF 		= new ArrayList<Double>();
			List<Double> nineFivePercCDF 	= new ArrayList<Double>();
			
			while ( (sCurrentLine = br.readLine()) != null )
			{
				String[] parsed = sCurrentLine.split(" ");
				
				double meanTime       = Double.parseDouble(parsed[3]);
				double medianTime     = Double.parseDouble(parsed[5]);
				double fivePerc       = Double.parseDouble(parsed[11]);
				double nineFivePerc   = Double.parseDouble(parsed[13]);
				
				meanCDF.add(meanTime);
				medianCDF.add(medianTime);
				fivePercCDF.add(fivePerc);
				nineFivePercCDF.add(nineFivePerc);
			}
			
			Double [] typeArray = new Double[1];
			typeArray[0] 		= new Double(0);
			
			Double[] meanArray = meanCDF.toArray(typeArray);
			Arrays.sort(meanArray);
			
			Double[] medianArray = medianCDF.toArray(typeArray);
			Arrays.sort(medianArray);
			
			Double[] fivePercArray = fivePercCDF.toArray(typeArray);
			Arrays.sort(fivePercArray);
			
			Double[] nineFivePercArray = nineFivePercCDF.toArray(typeArray);
			Arrays.sort(nineFivePercArray);
			
		
			File file = new File("interMobilityTimeCDF.txt");
			
			// if file doesn't exists, then create it
			if ( !file.exists() )
			{
				file.createNewFile();
			}
			
			FileWriter fw 	  = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			
			for( int i=0;i<meanArray.length;i++ )
			{
				double perce = (((i+1)*1.0)/(meanArray.length*1.0))*100.0;
				String write 
					= perce+","+meanArray[i]+","+medianArray[i]+","+fivePercArray[i]+","+nineFivePercArray[i]+"\n";
				bw.write(write);
			}
			
			bw.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		} finally
		{
			try
			{
				if (br != null)
					br.close();
			} catch (IOException ex)
			{
				ex.printStackTrace();
			}
		}
	}
	
	public void calculateDistanceCDFAcrossUsers()
	{
		BufferedReader br = null;
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader
					(new FileReader
				("/home/adipc/GNS/ContextServiceExperiments/interMobilityDistanceDist.txt"));
			List<Double> meanCDF   			= new ArrayList<Double>();
			List<Double> medianCDF 			= new ArrayList<Double>();
			List<Double> fivePercCDF 		= new ArrayList<Double>();
			List<Double> nineFivePercCDF 	= new ArrayList<Double>();
			
			while ( (sCurrentLine = br.readLine()) != null )
			{
				String[] parsed = sCurrentLine.split(" ");
				
				double meanDist       = Double.parseDouble(parsed[3]);
				double medianDist     = Double.parseDouble(parsed[5]);
				double fivePerc       = Double.parseDouble(parsed[11]);
				double nineFivePerc   = Double.parseDouble(parsed[13]);
				
				meanCDF.add(meanDist);
				medianCDF.add(medianDist);
				fivePercCDF.add(fivePerc);
				nineFivePercCDF.add(nineFivePerc);
			}
			
			Double [] typeArray = new Double[1];
			typeArray[0] 		= new Double(0);
			
			Double[] meanArray = meanCDF.toArray(typeArray);
			Arrays.sort(meanArray);
			
			Double[] medianArray = medianCDF.toArray(typeArray);
			Arrays.sort(medianArray);
			
			Double[] fivePercArray = fivePercCDF.toArray(typeArray);
			Arrays.sort(fivePercArray);
			
			Double[] nineFivePercArray = nineFivePercCDF.toArray(typeArray);
			Arrays.sort(nineFivePercArray);
			
		
			File file = new File("interMobilityDistanceCDF.txt");
			
			// if file doesn't exists, then create it
			if ( !file.exists() )
			{
				file.createNewFile();
			}
			
			FileWriter fw 	  = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			
			for( int i=0;i<meanArray.length;i++ )
			{
				double perce = (((i+1)*1.0)/(meanArray.length*1.0))*100.0;
				String write 
					= perce+","+meanArray[i]+","+medianArray[i]+","+fivePercArray[i]+","+nineFivePercArray[i]+"\n";
				bw.write(write);
			}
			
			bw.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		} finally
		{
			try
			{
				if (br != null)
					br.close();
			} catch (IOException ex)
			{
				ex.printStackTrace();
			}
		}
	}
	
	public void calculateWindowTrace() throws IOException
	{
		File file = new File("windowDistanceForUsers.txt");
		// if file doesn't exists, then create it
		if ( !file.exists() )
		{
			file.createNewFile();
		}
			
		FileWriter fw 	  = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		
		File file1 = new File("windowTransitionsForUsers.txt");
		// if file doesn't exists, then create it
		if (!file1.exists())
		{
			file1.createNewFile();
		}
			
		FileWriter fw1 	  = new FileWriter(file1.getAbsoluteFile());
		BufferedWriter bw1 = new BufferedWriter(fw1);
		
		Iterator<Integer> userIdIter = this.userMobilityEntryHashMap.keySet().iterator();
		
		while( userIdIter.hasNext() )
		{
			Integer userId = userIdIter.next();
			List<LogEntryClass> userEntryList = this.userMobilityEntryHashMap.get(userId);
			boolean firstOne = true;
			
			double beginTimestamp = 0;
			double distanceInCurrWindow = 0;
			
			double numTransitionInCurrWindow = 0;
			
			List<Double> windowDist = new ArrayList<Double>();
			this.windowDistanceHashMap.put(userId, windowDist);
			
			List<Double> windowTrans = new ArrayList<Double>();
			this.windowTransitionsHashMap.put(userId, windowTrans);
			
			GlobalCoordinate prevCoord = null;
			for( int i=0;i<userEntryList.size();i++ )
			{
				LogEntryClass logEntry = userEntryList.get(i);
				
				if(firstOne)
				{
					beginTimestamp = logEntry.getUnixTimeStamp();
					prevCoord = new GlobalCoordinate(logEntry.getLatitude(), logEntry.getLongitude());
					firstOne = false;
					continue;
				}
				else
				{
					double currTimeStamp = logEntry.getUnixTimeStamp();
					GlobalCoordinate currCoord = new GlobalCoordinate(logEntry.getLatitude(), 
							logEntry.getLongitude());
					
					if( (currTimeStamp-beginTimestamp) <= NomadLogProcessing.WeatherTraceNumMinutes)
					{
						GeodeticCalculator geoCalc  = new GeodeticCalculator();
						
						GeodeticCurve geodeticCurve 
										= geoCalc.calculateGeodeticCurve(Ellipsoid.WGS84, prevCoord, currCoord);
						
						double distanceInMeters = geodeticCurve.getEllipsoidalDistance();
						distanceInCurrWindow+=distanceInMeters;
						numTransitionInCurrWindow++;
					}
					else
					{
						windowTrans.add(numTransitionInCurrWindow);
						windowDist.add(distanceInCurrWindow);
						distanceInCurrWindow 	  = 0;
						numTransitionInCurrWindow = 0;
						beginTimestamp = logEntry.getUnixTimeStamp();
					}
					prevCoord = currCoord;
				}
			}
			
			Double [] typeArray = new Double[1];
			typeArray[0] = new Double(0);
			System.out.println("WindowDistanceList size "+windowDist.size());
			
			if( windowDist.size() > 0 )
			{
				StatClass statCls = new StatClass(windowDist.toArray(typeArray));
				bw.write("userId "+userId+" mean "+statCls.getMean()+" median "+statCls.getMedian()
				+" min "+statCls.getMin()+" max "+statCls.getMax()+" 5Perc "+statCls.get5Perc()
				+" 95Perc "+statCls.get95Perc()+"\n");
			}
			
			System.out.println("WindowTransitionList size "+windowTrans.size());
			
			if( windowTrans.size() > 0 )
			{
				StatClass statCls = new StatClass(windowTrans.toArray(typeArray));
				bw1.write("userId "+userId+" mean "+statCls.getMean()+" median "+statCls.getMedian()
				+" min "+statCls.getMin()+" max "+statCls.getMax()+" 5Perc "+statCls.get5Perc()
				+" 95Perc "+statCls.get95Perc()+"\n");
			}
		}
		bw.close();
		bw1.close();
	}
	
	public void calculateWindowDistanceCDFAcrossUsers()
	{
		BufferedReader br = null;
		try
		{
			String sCurrentLine;
			
			br 
			= new BufferedReader
			(new FileReader("/home/adipc/GNS/WeatherCaseStudy/windowDistanceForUsers.txt"));
			List<Double> meanCDF   			= new ArrayList<Double>();
			List<Double> medianCDF 			= new ArrayList<Double>();
			List<Double> fivePercCDF 		= new ArrayList<Double>();
			List<Double> nineFivePercCDF 	= new ArrayList<Double>();
			
			while ( (sCurrentLine = br.readLine()) != null )
			{
				String[] parsed = sCurrentLine.split(" ");
				
				double meanDist       = Double.parseDouble(parsed[3]);
				double medianDist     = Double.parseDouble(parsed[5]);
				double fivePerc       = Double.parseDouble(parsed[11]);
				double nineFivePerc   = Double.parseDouble(parsed[13]);
				
				meanCDF.add(meanDist);
				medianCDF.add(medianDist);
				fivePercCDF.add(fivePerc);
				nineFivePercCDF.add(nineFivePerc);
			}
			
			Double [] typeArray = new Double[1];
			typeArray[0] 		= new Double(0);
			
			Double[] meanArray = meanCDF.toArray(typeArray);
			Arrays.sort(meanArray);
			
			Double[] medianArray = medianCDF.toArray(typeArray);
			Arrays.sort(medianArray);
			
			Double[] fivePercArray = fivePercCDF.toArray(typeArray);
			Arrays.sort(fivePercArray);
			
			Double[] nineFivePercArray = nineFivePercCDF.toArray(typeArray);
			Arrays.sort(nineFivePercArray);
			
		
			File file = new File("windowMobilityDistanceCDF.txt");
			
			// if file doesn't exists, then create it
			if ( !file.exists() )
			{
				file.createNewFile();
			}
			
			FileWriter fw 	  = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			
			for( int i=0;i<meanArray.length;i++ )
			{
				double perce = (((i+1)*1.0)/(meanArray.length*1.0))*100.0;
				String write 
					= perce+","+meanArray[i]+","+medianArray[i]+","+fivePercArray[i]+","+nineFivePercArray[i]+"\n";
				bw.write(write);
			}
			
			bw.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		} finally
		{
			try
			{
				if (br != null)
					br.close();
			} catch (IOException ex)
			{
				ex.printStackTrace();
			}
		}
	}
	
	public void calculateWindowTransitionsCDFAcrossUsers()
	{
		BufferedReader br = null;
		try
		{
			String sCurrentLine;
			
			br 
				= new BufferedReader
				(new FileReader("/home/adipc/GNS/WeatherCaseStudy/windowTransitionsForUsers.txt"));
			List<Double> meanCDF   			= new ArrayList<Double>();
			List<Double> medianCDF 			= new ArrayList<Double>();
			List<Double> fivePercCDF 		= new ArrayList<Double>();
			List<Double> nineFivePercCDF 	= new ArrayList<Double>();
			
			while ( (sCurrentLine = br.readLine()) != null )
			{
				String[] parsed = sCurrentLine.split(" ");
				
				double meanDist       = Double.parseDouble(parsed[3]);
				double medianDist     = Double.parseDouble(parsed[5]);
				double fivePerc       = Double.parseDouble(parsed[11]);
				double nineFivePerc   = Double.parseDouble(parsed[13]);
				
				meanCDF.add(meanDist);
				medianCDF.add(medianDist);
				fivePercCDF.add(fivePerc);
				nineFivePercCDF.add(nineFivePerc);
			}
			
			Double [] typeArray = new Double[1];
			typeArray[0] 		= new Double(0);
			
			Double[] meanArray = meanCDF.toArray(typeArray);
			Arrays.sort(meanArray);
			
			Double[] medianArray = medianCDF.toArray(typeArray);
			Arrays.sort(medianArray);
			
			Double[] fivePercArray = fivePercCDF.toArray(typeArray);
			Arrays.sort(fivePercArray);
			
			Double[] nineFivePercArray = nineFivePercCDF.toArray(typeArray);
			Arrays.sort(nineFivePercArray);
			
		
			File file = new File("windowMobilityTransitionCDF.txt");
			
			// if file doesn't exists, then create it
			if ( !file.exists() )
			{
				file.createNewFile();
			}
			
			FileWriter fw 	  = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			
			for( int i=0;i<meanArray.length;i++ )
			{
				double perce = (((i+1)*1.0)/(meanArray.length*1.0))*100.0;
				String write 
					= perce+","+meanArray[i]+","+medianArray[i]+","+fivePercArray[i]+","+nineFivePercArray[i]+"\n";
				bw.write(write);
			}
			
			bw.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		} finally
		{
			try
			{
				if (br != null)
					br.close();
			} catch (IOException ex)
			{
				ex.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws IOException
	{
		NomadLogProcessing nomadLog = new NomadLogProcessing();
		nomadLog.readNomadLag();
		System.out.println( "Number of distinct users "
							+userMobilityEntryHashMap.size() );
		
		nomadLog.calculateInterMobilityTimeForAUser();
		//nomadLog.calculateWindowTrace();
		//nomadLog.calculateWindowTransitionsCDFAcrossUsers();
		//nomadLog.calculateWindowDistanceCDFAcrossUsers();
		//nomadLog.calculateInterMobilityTimeForAUser();
		//nomadLog.calculateInterMobilityDistanceForAUser();
		//nomadLog.calculateTimeCDFAcrossUsers();
		//nomadLog.calculateDistanceCDFAcrossUsers();
	}
}