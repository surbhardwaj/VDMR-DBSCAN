package dbscan;


public class Point 
{
	
	double[] attrib_value;						// value of the attributes for the points
	int index;									// index of the point
	public boolean isCorepoint=false;
	public boolean isCommon_point=false;		// identify the points which are common between the partitions used for merging
	public String partition_id=null;					// identify the partition id of the point assigned when the partition is created
	public String cluster_id=""+0;
	public double eps_value = 0;				// Point is clustered using the eps_value epsilon value
	public int cluster = 0;
	public int flag = 0;
	public double boundary_kdist = 0;				// Kth nearest point distance for boundary points
	public Point(String arr)
	{
		
			String []Linearr;
			Linearr = arr.split("\t");
			int len = Linearr.length;
			attrib_value=new double[len-3];
			for(int j=1;j<len-2;j++)
			{	
				this.attrib_value[j-1]=Double.valueOf(Linearr[j]);
				
			}
			
			this.index=Integer.valueOf(Linearr[0]);
			this.partition_id = "P"+Linearr[len-2];
			if(Linearr[len-1].equals("1"))
				this.isCommon_point = true;
			else
				this.isCommon_point = false;
			
	}
	
	
	public double[] toDouble() 
	{
		double[] xy =attrib_value;
		return xy; 
	}
	
	
}
