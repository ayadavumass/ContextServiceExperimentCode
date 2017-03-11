package edu.umass.cs.mysqlBenchmarking;

import java.util.HashMap;

public class UpdateInfoClass
{
	private final String guid;
	private final HashMap<String, String> attrValMap;
	
	public UpdateInfoClass(String guid, HashMap<String, String> attrValMap)
	{
		this.guid = guid;
		this.attrValMap = attrValMap;
	}
	
	public String getGUID()
	{
		return this.guid;
	}
	
	public HashMap<String, String> getAttrValMap()
	{
		return attrValMap;
	}
}