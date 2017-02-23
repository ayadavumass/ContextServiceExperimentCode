package edu.umass.cs.largescalecasestudy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.hash.Hashing;

import edu.umass.cs.acs.geodesy.GeodeticCalculator;
import edu.umass.cs.acs.geodesy.GeodeticCurve;
import edu.umass.cs.acs.geodesy.GlobalCoordinate;

public class UserInitializationClass extends AbstractRequestSendingClass
{
	// different random generator for each variable, as using one for all of them
	// doesn't give uniform properties.
	private final Random initRand;
	
	private HashMap<String, JSONObject> firstJSONObjectMap;
	
	public UserInitializationClass()
	{
		super( LargeNumUsers.INSERT_LOSS_TOLERANCE );
		firstJSONObjectMap = new HashMap<String, JSONObject>();
		initRand = new Random((LargeNumUsers.myID+1)*100);
		
		readFirstEntriesAfterStartTime();
	}
	
	private void sendAInitMessage(long guidNum, BufferedWriter bw) throws Exception
	{
		double randnum = initRand.nextDouble();
		
		CountyNode countynode = LargeNumUsers.binarySearchOfCounty(randnum);
		
		double latitude =  countynode.minLat + 
					(countynode.maxLat - countynode.minLat) * initRand.nextDouble();
		
		double longitude = countynode.minLong + 
					(countynode.maxLong - countynode.minLong) * initRand.nextDouble();
		
		
		JSONObject attrValJSON = new JSONObject();
		
		attrValJSON.put(LargeNumUsers.LATITUDE_KEY, latitude);
		attrValJSON.put(LargeNumUsers.LONGITUDE_KEY, longitude);
		
		
		String userGUID = LargeNumUsers.getSHA1(LargeNumUsers.guidPrefix+guidNum);
		
		computeAndStoreTransfromFromUserLog(userGUID, latitude, 
				longitude, bw);
		
		
		ExperimentUpdateReply updateRep = new ExperimentUpdateReply(guidNum, userGUID);
		
		LargeNumUsers.csClient.sendUpdateWithCallBack
										( userGUID, null, attrValJSON, 
										 -1, updateRep, this.getCallBack() );
	}
	
	
	private void computeAndStoreTransfromFromUserLog(String userGUID, double latitude, 
														double longitude, BufferedWriter bw)
	{
		int arrayIndex = Hashing.consistentHash(userGUID.hashCode(), 
											LargeNumUsers.filenameList.size());
		
		String filename = LargeNumUsers.filenameList.get(arrayIndex);
		
		JSONObject firstJSON = firstJSONObjectMap.get(filename);
		
		try
		{	
			JSONObject geoLocJSON = firstJSON.getJSONObject(LargeNumUsers.GEO_LOC_KEY);
			JSONArray coordArray = geoLocJSON.getJSONArray(LargeNumUsers.COORD_KEY);
			
			double logLat  = coordArray.getDouble(1);
			double logLong = coordArray.getDouble(0);
			
			GlobalCoordinate transformCoord 
							= new GlobalCoordinate(latitude, longitude);
			
			GlobalCoordinate logCoord
							= new GlobalCoordinate(logLat, logLong);
			
			GeodeticCurve gCurve 
				= GeodeticCalculator.calculateGeodeticCurve(logCoord, transformCoord);

			double startAngle = gCurve.getAzimuth();
			//endAngle = gCurve.getReverseAzimuth();
			double distanceInMeters = gCurve.getEllipsoidalDistance();

			UserRecordInfo userRecInfo = new UserRecordInfo();
			
			userRecInfo.filename = filename;
			userRecInfo.distanceInMeters = distanceInMeters;
			userRecInfo.startAngle = startAngle;
			
			//LargeNumUsers.userInfoMap.put(userGUID, userRecInfo);
			String str = userGUID+","+filename+","+distanceInMeters+","+startAngle+"\n";
			bw.write(str);
		}
		catch (JSONException e) 
		{
			e.printStackTrace();
		} catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	
	public void initializaRateControlledRequestSender() throws Exception
	{
		this.startExpTime();
		double reqspms = LargeNumUsers.initRate/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		long totalNumUsersSent = 0;
		BufferedWriter bw = null;
		
		try
		{
			bw = new BufferedWriter(new FileWriter(LargeNumUsers.USER_INFO_FILE_NAME));
			
			while(  totalNumUsersSent < LargeNumUsers.numusers  )
			{
				for(int i=0; i<numberShouldBeSentPerSleep; i++ )
				{
					sendAInitMessage(totalNumUsersSent, bw);
					totalNumUsersSent++;
					numSent++;
					assert(numSent == totalNumUsersSent);
					if(totalNumUsersSent >= LargeNumUsers.numusers)
					{
						break;
					}
				}
				if(totalNumUsersSent >= LargeNumUsers.numusers)
				{
					break;
				}
				currTime = System.currentTimeMillis();
				
				double timeElapsed = ((currTime- expStartTime)*1.0);
				double numberShouldBeSentByNow = timeElapsed*reqspms;
				double needsToBeSentBeforeSleep = numberShouldBeSentByNow - numSent;
				if(needsToBeSentBeforeSleep > 0)
				{
					needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
				}
				
				for(int i=0;i<needsToBeSentBeforeSleep;i++)
				{
					sendAInitMessage(totalNumUsersSent, bw);
					totalNumUsersSent++;
					numSent++;
					assert(numSent == totalNumUsersSent);
					if(totalNumUsersSent >= LargeNumUsers.numusers)
					{
						break;
					}
				}
				
				if(totalNumUsersSent >= LargeNumUsers.numusers)
				{
					break;
				}
				Thread.sleep(100);
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
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("UserInit eventual sending rate "+sendingRate);
		
		waitForFinish();
		
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("UserInit result:Goodput "+sysThrput);	
	}
	
	
	public void writeTraceToFile(int numEntries)
	{
		BufferedWriter bw = null;
		
		try 
		{
			bw = new BufferedWriter(new FileWriter("nationwidePopTrace.txt"));
			
			for(int i=0; i<numEntries; i++)
			{	
				double randnum = initRand.nextDouble();
				
				CountyNode countynode = LargeNumUsers.binarySearchOfCounty(randnum);
				
				
				double latitude =  countynode.minLat + 
							(countynode.maxLat - countynode.minLat) * initRand.nextDouble();
				
				double longitude = countynode.minLong + 
							(countynode.maxLong - countynode.minLong) * initRand.nextDouble();
				
				bw.write("latitude,"+latitude+",longitude,"+longitude+"\n");
			}
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		} finally 
		{
			try 
			{
				if (bw != null)
					bw.close();
			}
			catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
	}
	
	public void readFirstEntriesAfterStartTime()
	{
		for(int i=0; i<LargeNumUsers.filenameList.size(); i++)
		{
			String filename = LargeNumUsers.filenameList.get(i);
			
			BufferedReader readfile = null;
			
			try
			{
				String sCurrentLine;
				readfile = new BufferedReader(new FileReader
							(LargeNumUsers.USER_TRACE_DIR+"/"+filename));
				
				while( (sCurrentLine = readfile.readLine()) != null )
				{
					try
					{
						JSONObject jsoObject = new JSONObject(sCurrentLine);
						
						double geoloctimestamp = Double.parseDouble
								(jsoObject.getString(LargeNumUsers.GEO_LOC_TIME_KEY));
						
						if( geoloctimestamp >= LargeNumUsers.START_UNIX_TIME )
						{
							firstJSONObjectMap.put(filename, jsoObject);
							break;
						}
					}
					catch (NumberFormatException e) 
					{
						e.printStackTrace();
					} 
					catch (JSONException e) 
					{
						e.printStackTrace();
					}
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
		}
	}
	
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			if(numRecvd%10000 == 0)
			{
				System.out.println("UserInit reply recvd "+userGUID+" time taken "+timeTaken+
					" numSent "+numSent+" numRecvd "+numRecvd);
			}
			//if(currNumReplyRecvd == currNumReqSent)
			if(checkForCompletionWithLossTolerance(numSent, numRecvd))
			{
				waitLock.notify();
			}
		}
	}
	
	@Override
	public void incrementSearchNumRecvd(int resultSize, long timeTaken)
	{
	}
}