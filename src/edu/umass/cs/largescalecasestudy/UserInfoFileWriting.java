package edu.umass.cs.largescalecasestudy;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import com.google.common.hash.Hashing;


public class UserInfoFileWriting 
{
	// different random generator for each variable, as using one for all of them
	// doesn't give uniform properties.
	private final Random initRand;
	
	public UserInfoFileWriting()
	{
		//firstJSONObjectMap = new HashMap<String, JSONObject>();
		initRand = new Random((LargeNumUsers.myID+1)*100);	
		//readFirstEntriesAfterStartTime();
	}
	
	
	private void sendAInitMessage(long guidNum, BufferedWriter bw) throws Exception
	{
		double randnum = initRand.nextDouble();
		
		CountyNode countynode = LargeNumUsers.binarySearchOfCounty(randnum);
		
		double homeLat =  countynode.minLat + 
					(countynode.maxLat - countynode.minLat) * initRand.nextDouble();
		
		double homeLong = countynode.minLong + 
					(countynode.maxLong - countynode.minLong) * initRand.nextDouble();
		
		String userGUID = LargeNumUsers.getSHA1(LargeNumUsers.guidPrefix+guidNum);
		
		int arrayIndex = Hashing.consistentHash(userGUID.hashCode(), 
				LargeNumUsers.filenameList.size());
		
		String filename = LargeNumUsers.filenameList.get(arrayIndex);
		int numUpdatesPerDay = DistributionLearningFromTraces.getNumUpdatesFromDistForUser
																					(filename);
		int nextUpdateNum   = 1;
		long nextUpdateTime = DistributionLearningFromTraces.getNextUpdateTimeFromDist
																(filename, nextUpdateNum);
		
		nextUpdateTime = LargeNumUsers.distributeTimeUniformly(nextUpdateTime);
		
		boolean inTimeslot = LargeNumUsers.checkIfRelativeTimeInTimeSlot(nextUpdateTime);
		
		//DistanceAndAngle distAngle 
		//				= DistributionLearningFromTraces.getDistAngleFromDist
		//												(filename, nextUpdateNum);
		//GlobalCoordinate destCoord = GeodeticCalculator.calculateEndingGlobalCoordinates
		//		( new GlobalCoordinate(homeLat, homeLong), 
		//				distAngle.angle, distAngle.distance );
		
		if(inTimeslot)
		{
			UserRecordInfo userRecInfo = new UserRecordInfo( userGUID, filename, 
				homeLat, homeLong, numUpdatesPerDay,
				nextUpdateNum, nextUpdateTime, 
				homeLat, homeLong );
			
			try
			{
				bw.write(userRecInfo.toString() +"\n");
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	
	public void initializaRateControlledRequestSender() throws Exception
	{
		BufferedWriter bw = null;
		
		try
		{
			String writeFileName = LargeNumUsers.USER_INFO_FILE_PREFIX
												+LargeNumUsers.userinfoFileNum;
			long totalNumUsersSent = 0;
			bw = new BufferedWriter(new FileWriter(writeFileName));
			
			while(  totalNumUsersSent < LargeNumUsers.numusers  )
			{
				sendAInitMessage(totalNumUsersSent, bw);
				totalNumUsersSent++;
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
		
		System.out.println("FileBasedUserInit complete eventual sending rate ");
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
	
}