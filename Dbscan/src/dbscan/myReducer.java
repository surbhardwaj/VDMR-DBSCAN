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
 


public class myReducer extends Reducer<IntWritable, Text, IntWritable, Text> 
{
  
 
	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException 
	{
		
		ArrayList<Double>eps = new ArrayList<Double>();				// holds the epsilon values for each cluster id	
		ArrayList<Double>eps_dif = new ArrayList<Double>();			// holds the epsilon threshold for merging for each cluster
		ArrayList<Double>k_distratio = new ArrayList<Double>();		// ratio of epsilon and k-distance value for each boundary point   
		ArrayList<Boolean>Core = new ArrayList<Boolean>();			// if a point is a core point or not
		ArrayList<String> cluster_id = new ArrayList<String>();			// clusters obtained for a key value 
		ArrayList<String>Merge	= new ArrayList<String>();			//List of clusters which can be merged
		ArrayList<String>Not_Merge = new ArrayList<String>();		// List of clusters which cannot be merged
		//ArrayList<Double>min_eps = new ArrayList<Double>();
		LinkedHashMap<Integer,String>point_record = new LinkedHashMap<Integer,String>();
		 
		double thresh_global = (1.8871949374160983+0.7937121338349264+1.864516463163823+2.269445237268947)/4.0;

		//(0.8197098163274144 + 1.63855493306089 + 1.4891024888911404 + 0.41073399787195375)/4.0;

																																// [2.127886328780783, 1.3653822791344747, 0.45004978582007893]


		
		// Fetch the values from iterator
		
		Iterator<Text>iterator = values.iterator();
		Configuration conf = new Configuration();							// writing output to HDFS
	    FileSystem fs = FileSystem.get(conf);
	    Path inputfile = new Path("in/map");
	    BufferedWriter getdatabuffer;
		   // boolean flag = Boolean.getBoolean(fs.getConf().get("dfs.support.append"));
		    //System.out.println("dfs.support.append is set to be " + flag);
		    if(!fs.exists(inputfile))
		    {
		    	getdatabuffer = new BufferedWriter(new OutputStreamWriter(fs.create(inputfile)));
		    }
		    else
		    {
		    	getdatabuffer = new BufferedWriter(new OutputStreamWriter(fs.append(inputfile)));
		    }
		
		    if(Integer.valueOf(String.valueOf(key))==-1)
		    {
		    	//while(iterator.hasNext())
				//{
					
				
					//Text first = (Text)iterator.next();
					//min_eps.add(Double.valueOf(String.valueOf(first)));
				//}
		    	
		    	
		    }
		    else
		    {
		    
		
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
		    			eps.add(0.0);
		    			eps_dif.add(0.0);
		    			k_distratio.add(0.0);
		    			point_record.put(Integer.valueOf(""+key), val[2]+"\t"+val[3]+"\t");
				
				
		    		}
		    		else
		    		{
		    			cluster_id.add(val[0]);
		    			Core.add(Boolean.valueOf(val[1]));
		    			eps.add(Double.valueOf(val[2]));
		    			eps_dif.add(Double.valueOf(val[3]));
		    			k_distratio.add(Double.valueOf(val[4]));
		    			point_record.put(Integer.valueOf(""+key), val[6]+"\t"+val[7]+"\t");
		    			//getdatabuffer.write(Integer.valueOf(""+key)+","+val[5]);
		    			//getdatabuffer.write("\n");
		    	
						
			
		    		}
			
		    	}	// While loop end
		    }
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
			for(int k=j+1;k<len;k++)
			{
				// Check if one of the point is a core point
				if(Core.get(j)==true || Core.get(k)==true)
				{
					// if one point is core and other point is a noise point
					if(cluster_id.get(j).equals("-1") || cluster_id.get(j).equals("0"))
					{
						Not_Merge.add(cluster_id.get(j)+"\t"+cluster_id.get(k));		// Clusters which cannot be merged because one is a noise point
						
					}
					else if(cluster_id.get(k).equals("-1") || cluster_id.get(k).equals("0"))
					{
						
						Not_Merge.add(cluster_id.get(j)+"\t"+cluster_id.get(k));		// Clusters which cannot be merged because one is a noise point
						
					}
					else
					{
						
					/* If one point is core or both points are core and none is noise point */
					double diff=0.0;
					if(eps.get(j)>eps.get(k))
					{
						diff = eps.get(j)-eps.get(k);		// difference between epsilon value of two clusters
					}
					else
						diff = eps.get(k)-eps.get(j);
						
					/*double min=0;
					if(eps_dif.get(j)<eps_dif.get(k))
					{
						min = eps_dif.get(j);
					}
					else
						min = eps_dif.get(k);*/
					
					// if the density difference is less than 50 % of the density difference then can be merged
					if(diff<=thresh_global)		// threshold
					{
						double min_1 = 0;
						/*if(eps.get(j)<eps.get(k))
							min_1 = eps.get(j);
						else
							min_1 = eps.get(k);	*/						// min_1 will be the new epsilon value for the merged cluster
						
						//int total = no_point.get(j)+no_point.get(k);	// Total no. of points in the merged cluster
						String st = point_record.get(Integer.valueOf(""+key))+cluster_id.get(j)+"-"+cluster_id.get(k)+"\t"+eps.get(j)+"\t"+eps.get(k);
						Merge.add(st);
						
					}
					else
					{
						Not_Merge.add(cluster_id.get(j)+"\t"+cluster_id.get(k));		// Clusters which cannot be merged 
						
					}
					
					}// end of else
				} // end if
				else 
				{
					// If both the points are not core or are noise points
					
					Not_Merge.add(cluster_id.get(j)+"\t"+cluster_id.get(k));		// Clusters which cannot be merged
					
				}
					
				
				
				
				
			}
		}
		
	/*-----------------------------------------------------------------------------------------------------------------------*/
		
		// Passing the Merge_lists or assigning the value to the common point if it do not helps in merging
		
		
		//getdatabuffer.write(Integer.valueOf(""+key)+","+min_eps);
		//getdatabuffer.write("\n");
		//getdatabuffer.write(Integer.valueOf(""+key)+","+Not_Merge);
		//getdatabuffer.write("\n");
		
		
		if(Merge.isEmpty()==false)
		{
			
			for(int i=0;i<Merge.size();i++)
			{
				
				// Output all the merge combinations which can be formed
				context.write(key,new Text(Merge.get(i)));
				
			}
			
			
		}
		else
		{
			//getdatabuffer.write(""+Not_Merge);
			
			ArrayList<String>cluster = new ArrayList<String>();
			
			for(int i=0;i<Not_Merge.size();i++)
			{
				
				String str[] = Not_Merge.get(i).split("\t");
				
				for(int j=0;j<str.length;j++)
				{
					
					cluster.add(str[j]);
					
				}
				
				//getdatabuffer.write("\n");
			}
			
			
			// Removing duplicates from the cluster arraylist
			
			HashSet hs = new HashSet();
			hs.addAll(cluster);
			cluster.clear();
			cluster.addAll(hs);
			String cid = null;
			ArrayList<Double>value = new ArrayList<Double>();
			
			// Duplicates removed 
			
			// Check whether cluster list contains all the noise values 
			
			
			cluster.remove("-1");
			cluster.remove("0");
			// Cluster had only -1 and 0 as values 
			if(cluster.isEmpty()== true)
			{
				cid = "-1";
			}
			else
			{
				// Cluster has values other than noise
				if(cluster.size()==1)
				{
					cid = cluster.get(0); //if values left is the only valid cluster-id
				}
				else
				{
					// assign to the the cluster which has min value of (epsilon/K-Dist) ratio value
					
					for(int i=0;i<cluster.size();i++)
					{	
						int index = cluster_id.indexOf(cluster.get(i));
						value.add(k_distratio.get(index));
						
					}
					Collections.sort(value);
					double val = value.get(0);			// smallest value from the List
					int ind = k_distratio.indexOf(val);
					cid = cluster_id.get(ind);
					value.clear();
					//cid=cluster.get(1);
					
				}
				
				
				
			}// end of else
			
			if(key!=new IntWritable(-1))
			{
				getdatabuffer.write(Integer.valueOf(""+key)+"\t"+point_record.get(Integer.valueOf(""+key))+cid);
				getdatabuffer.write("\n");
			}
    		//getdatabuffer.close();
			
			
		}// end of main else i.e if Merge list is empty
		
		/*--------------------------------------------------------------------------------------------------------------------*/
		
		getdatabuffer.close();
		
		
		
		
		
		
		
		
		
		//context.write(key,new Text("HIiiiiii"));
	}
}
	