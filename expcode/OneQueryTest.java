package edu.umass.cs.expcode;
import edu.umass.cs.msocket.geocast.MSocketGroupWriter;


public class OneQueryTest
{
	public static void main()
	{
		try 
		{
			MSocketGroupWriter testWrit = new MSocketGroupWriter("writer1", "1 <= contextATT0 <= 1500");
			ContextServiceLogger.getLogger().fine("printing group members");
			testWrit.printGroupMembers();
			testWrit.close();
		} catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
}