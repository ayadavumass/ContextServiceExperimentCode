package edu.umass.cs.ubercasestudy;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;


public class TaxiDataProcessing 
{
	public static final String TAXI_DATA_PATH		
					= "/home/adipc/Documents/NYCTaxiData/yellow_tripdata_2016-02.csv";
	
	//private long currLineNum;
	
	private HashMap<String, Long> perDayTaxiPickUps;
	
	public TaxiDataProcessing()
	{
		//currLineNum = 0;
		perDayTaxiPickUps = new HashMap<String, Long>();
	}
	
	public void processData()
	{
		
	}
	
	public void readTheTaxiFile()
	{
		BufferedReader br = null;
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader(new FileReader(TAXI_DATA_PATH));
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				String[] lineParsed = sCurrentLine.split(",");
				String dateTime = lineParsed[1];
				
				String[] dateParsed = dateTime.split(" ");
				String date = dateParsed[0];
				
				if( perDayTaxiPickUps.containsKey(date) )
				{
					long currNum = perDayTaxiPickUps.get(date);
					currNum++;
					perDayTaxiPickUps.put(date, currNum);
				}
				else
				{
					perDayTaxiPickUps.put(date, new Long(1));
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
				if ( br != null )
					br.close();
			} catch ( IOException ex )
			{
				ex.printStackTrace();
			}
		}
		
		Iterator<String> dateIter = perDayTaxiPickUps.keySet().iterator();
		
		while( dateIter.hasNext() )
		{
			String date = dateIter.next();
			
			long totalPickup = perDayTaxiPickUps.get(date);
			System.out.println("Date "+date+" totalPickup "+totalPickup);
		}
	}
	
	public static void main(String[] args)
	{
		TaxiDataProcessing taxiData = new TaxiDataProcessing();
		taxiData.readTheTaxiFile();	
	}
}