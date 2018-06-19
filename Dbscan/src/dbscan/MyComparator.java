package dbscan;


import java.util.Comparator;



public class MyComparator implements Comparator<node> 
{

	public int compare(node no, node ns)
	{
	       if(no.get_value() <  ns.get_value()) return -1;
	       if(no.get_value() == ns.get_value()) return 0;
	       return 1;

	
		
	}

}