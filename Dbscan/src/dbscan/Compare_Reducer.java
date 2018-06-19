package dbscan;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

 

 /*------------------------------------------------------------------------------------------------------------------------*/
	

// Reducer

// Input to the Reducer is int form of // (Pt_index,partition_id+Cid+isCorePoint+eps_value+eps_diff+epsilon & kth dist ratio)
// If point is a noise point or unclassified (cid=0 || cid=-1) then (Pt_index, clsuter_id)
 


public class Compare_Reducer extends Reducer<IntWritable, Text, IntWritable, Text> 
{
  
 
	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException 
	{
		
			
		
		   
		ArrayList<Boolean>Core = new ArrayList<Boolean>();			// if a point is a core point or not
		ArrayList<String> cluster_id = new ArrayList<String>();			// clusters obtained for a key value 
		ArrayList<String>Merge	= new ArrayList<String>();			//List of clusters which can be merged
		int flag_merge=0;
		//ArrayList<Double>min_eps = new ArrayList<Double>();
		LinkedHashMap<Integer,String>point_record = new LinkedHashMap<Integer,String>();
		 
		
/*-----------------------------------------------------------------------------------------------------------------------------------------*/		
		
		/*----- Fetch the values from iterator----*/
		
		Iterator<Text>iterator = values.iterator();
		Configuration conf = new Configuration();							// writing output to HDFS
	    FileSystem fs = FileSystem.get(conf);
	    Path inputfile = new Path("in/map");
	    BufferedWriter getdatabuffer;
		   
		    if(!fs.exists(inputfile))
		    {
		    	getdatabuffer = new BufferedWriter(new OutputStreamWriter(fs.create(inputfile)));
		    }
		    else
		    {
		    	getdatabuffer = new BufferedWriter(new OutputStreamWriter(fs.append(inputfile)));
		    }
		
		   // String st = val[2]+"\t"+val[3]+"\t";
		
		    	while(iterator.hasNext())
		    	{
			
		
		    		Text first = (Text)iterator.next();
		    		String val[]= (String.valueOf(first)).split("\t");
		    		//getdatabuffer.write(Integer.valueOf(""+key)+","+String.valueOf(first));
		    		
		    		//getdatabuffer.write("\n");
		    		if(val[0].equals("-1") || val[0].equals("0"))
		    		{
				
		    			cluster_id.add(val[0]);
		    			Core.add(false);			
		    			point_record.put(Integer.valueOf(""+key), val[2]+"\t"+val[3]+"\t");
				
				
		    		}
		    		else
		    		{
		    			cluster_id.add(val[0]);
		    			Core.add(Boolean.valueOf(val[1]));
		    			point_record.put(Integer.valueOf(""+key), val[2]+"\t"+val[3]+"\t");
		    			//getdatabuffer.write(Integer.valueOf(""+key)+","+val[5]);
		    			//getdatabuffer.write("\n");
		    	
						
			
		    		}
			
		    	}	// While loop end
		    
/*--------------------------------------------------------------------------------------------------------------------------*/
		int len = cluster_id.size();			// Size of the list of points having same common point
		
		// check if clusters can be merged or not
		 
		/*LinkedHashMap K_distmap = new LinkedHashMap(); // K_distmap stores the key, value pair as cluster
		
		for(int i=0;i<k_distratio.size();i++)
		{
			K_distmap.put(cluster_id.get(i), k_distratio.get(i));
			
		}*/
		
		for(int j=0;j<len;j++)
		{
			if(Core.get(j)==true)
				flag_merge=1;
			
			if(!Merge.contains(cluster_id.get(j)) && !(cluster_id.get(j).equals("-1")))
			{
				Merge.add(cluster_id.get(j));
				
			}
			
			
		}
		
		
	   
		if(flag_merge==1)
			context.write(key, new Text(point_record.get(Integer.valueOf(""+key))+Merge));
		else
		{
			if(Merge.isEmpty()==true)
			{
				getdatabuffer.write(Integer.valueOf(""+key)+"\t"+point_record.get(Integer.valueOf(""+key))+"-1");
				getdatabuffer.write("\n");
			}
			else
			{
				getdatabuffer.write(Integer.valueOf(""+key)+"\t"+point_record.get(Integer.valueOf(""+key))+Merge.get(0));
				getdatabuffer.write("\n");
			}
			
			
		}
	
		
		
	
		
			
		
			
		getdatabuffer.close();
			
    		//getdatabuffer.close();
			
			
		}
		
		/*--------------------------------------------------------------------------------------------------------------------*/
		
		
		
		
		
		
		
	
		
		
		
		//context.write(key,new Text("HIiiiiii"));
	}

	