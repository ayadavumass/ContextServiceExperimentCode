package edu.umass.cs.largescalecasestudy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.google.common.hash.Hashing;


public class UserInfoFileWriting
{
	// different random generator for each variable, as using one for all of them
	// doesn't give uniform properties.
	
	public UserInfoFileWriting()
	{
	}
	
	private void assingTraceAndWriteToFile(String guid, double homeLat, double homeLong, 
												BufferedWriter bw) throws Exception
	{	
		int arrayIndex = Hashing.consistentHash(guid.hashCode(), 
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
			UserRecordInfo userRecInfo = new UserRecordInfo( guid, filename, 
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
	
	public void initializeFileWriting() throws Exception
	{
		BufferedReader br = null;
		BufferedWriter bw = null;
		String writeFileName = LargeNumUsers.USER_INFO_FILE_PREFIX
				+LargeNumUsers.userinfoFileNum;
		
		try
		{
			bw = new BufferedWriter(new FileWriter(writeFileName));
			br = new BufferedReader(new FileReader(LargeNumUsers.guidFilePath));
			
			//first line is comment that starts with #
			String currLine = br.readLine();
			long startLineNum = LargeNumUsers.myID*LargeNumUsers.numusers;
			long endLineNum = (LargeNumUsers.myID+1)*LargeNumUsers.numusers;
			long currLineNum = 0;
			
			while( (currLine = br.readLine()) != null )
			{
				if( (currLineNum >= startLineNum) && (currLineNum < endLineNum) )
				{
					String[] parsed = currLine.split(",");
					String guid = parsed[0];
					double homeLat = Double.parseDouble(parsed[1]);
					double homeLong = Double.parseDouble(parsed[2]);
					
					assingTraceAndWriteToFile(guid, homeLat, homeLong, bw);
				}
				currLineNum++;
			}
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
		}
		finally
		{
			if(br != null)
			{
				br.close();
			}
			
			if(bw != null)
			{
				bw.close();
			}
		}	
		System.out.println("FileBasedUserInit complete eventual sending rate ");
	}
}