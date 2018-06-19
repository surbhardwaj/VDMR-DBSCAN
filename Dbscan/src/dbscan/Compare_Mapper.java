package dbscan;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import kd_package.KDTree;
import kd_package.KeyDuplicateException;
import kd_package.KeySizeException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Compare_Mapper extends Mapper<NullWritable, Text, IntWritable, Text> 
{
	
	
	@SuppressWarnings("unchecked")
	public void map(NullWritable key, Text value,Context contex) throws IOException, InterruptedException 
	{
		
		
		int len=0;
	 	String lines = value.toString();
	    String []lineArr = lines.split("\n"); 					// LineArr contains the array of records in the dataset separated by tab.
	    len = lineArr.length;								
		@SuppressWarnings("unused")
		MyComparator comparator = new MyComparator();			// For sorting the ArrayList value based on the distance value.
		Point []p;												// Array of point objects
		KDTree kdtree = new KDTree(2);							// 8 is the dimensions of the dataset
	    p=new Point[len];
		HashMap<Integer,Double>KD_Dist = new HashMap<Integer, Double>();
		for(int i=0;i<len;i++)
		{
			
			p[i]=new Point(lineArr[i]);
			
		}
		
		
		
		
/*--------------------------------------------------------------------------------------------------------------------------*/
		/* Create KDtree for the data points **/
		
		
		for(int i = 0; i < len ; i++) 
		{
			try {
				
				
				kdtree.insert(p[i].toDouble(), i+1);
			} catch(Exception | KeySizeException | KeyDuplicateException e) {}
		}
		
		
		
/*----------------------------------------------------------------------------------------------------------------------------*/		
		/* HDFS directory initialization */
		
		
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
	    
	    
	   
		    
 
/*---------------------------------------------------------------------------------------------------------------------------*/		    
		     
		    
		    /* Applying DBSCAN on the eps values and data points obtained */
		    
		    ArrayList<Integer>val = new ArrayList<Integer>();
		    ArrayList<Double> used_eps = new ArrayList<Double>();
		    int to=0;
		    
		    // Variable declaration
		    double epsilon = 0.5;
		    int min_points = 4;
		    Point current_p, result_p;
		    int cid	= 0;					// counter for the cluster id
		    
		    
		    
		    // DBSCAN code
		    // p[i].cluster contains the integer part of cluster id
		    // p[i].cluster_id contains the cluster id with partition
		   // int flag = 0;
		    //p[1].flag=1;
		    for(int k=0;k<p.length;k++)
		    {
		    	
		    		current_p = p[k];
		    		
		    		if (current_p.cluster==0 || current_p.cluster==-1)		// cluster_id is -1 for noise, 0 for unclassified and other values are for cluster id
		    		{
		    			// Cluster expansion part
		    			
		    			//System.out.println("Current index ---->"+k);
		    			try
						{
							// Finding points in the epsilon neighbourhood of a point
							List<Integer>seeds = kdtree.nearestEuclidean(current_p.toDouble(), epsilon);
							
							//System.out.println(seeds);
							int count=0;
							for(int f=0;f<seeds.size();f++)
							{
								if(p[seeds.get(f)-1].cluster_id.equals("0")|| p[seeds.get(f)-1].cluster_id.equals("-1"))
									count=count+1;
								
							}
							
							if((count)<min_points)		// 1 is subtracted because point itself is also included	
							{
								// no core point
								current_p.cluster_id = ""+-1;
								current_p.cluster = -1;
								current_p.isCorepoint = false;
								//System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1");
								continue;
								
							}
							else
							{
								// if point is a core point
								// all point in thye seed are density reachable
								// from the point
								to=0;
								cid = cid+1;
								
								for(int h=0;h<seeds.size();h++)  // last element is the point itself
								{
									int ind = seeds.get(h);
									if(p[ind-1].cluster== 0 || p[ind-1].cluster == -1)
									{
										p[ind-1].cluster_id =p[ind-1].partition_id+"C"+""+cid;
										p[ind-1].cluster = cid;
										p[ind-1].eps_value = epsilon;
										to=to+1;
									}
									
								}
								current_p.isCorepoint=true;
								//System.out.println(seeds);
								seeds.remove(seeds.indexOf(k+1));			// it removes the object with given value from the list
								
								while(!(seeds.isEmpty()))
								{
									current_p = p[seeds.get(0)-1];	// fetching first elemnt from the list
									List<Integer>result = kdtree.nearestEuclidean(current_p.toDouble(), epsilon);
									
									if((result.size())>=min_points)
									{
										for(int z=0;z<result.size();z++)		// take care of the point itself by subtracting 1
										{
											result_p = p[result.get(z)-1];
											
											if(result_p.cluster == 0 || result_p.cluster==-1)
											{
												
												if(result_p.cluster==0)
													seeds.add(result.get(z));
												
												result_p.cluster_id = result_p.partition_id+"C"+""+cid;
												result_p.cluster = cid;
												result_p.eps_value=epsilon;
												to=to+1;
												
												
											}
											
										}
										current_p.isCorepoint = true;
										
									}
									
									
									seeds.remove(0);		// removes element at index 0 in seeds list
								
								}				// end of while loop
								
								//continue;
								val.add(to);
						  }
							
							
							
					}
		    		catch(Exception | KeySizeException e){ System.out.println(e);}
		    			
		    	}
		    		else
		    			continue;
		    
		 }
		 // End of DBSCAN part 
		
/*-----------------------------------------------------------------------------------------------------------------------------*/
		    
		   	    
		
		   /* Output writing part */
		    
		    /*Configuration conf = new Configuration();							// writing output to HDFS
		    FileSystem fs = FileSystem.get(conf);
		    Path inputfile = new Path("in/map");
		    BufferedWriter getdatabuffer = new BufferedWriter(new OutputStreamWriter(fs.create(inputfile)));*/
		    
		   
		   
		   for(int s=0; s<len;s++ )
		    {
			   //getdatabuffer.write(p[s].index+","+p[s].cluster_id);
			   //getdatabuffer.write("\n");
		    		if(p[s].isCommon_point==false)
		    		{
		    		String string=String.valueOf(p[s].attrib_value[0]);
		    		string = string+"\t";
		    		for(int n2=1;n2<p[s].attrib_value.length;n2++)
		    		{	
		    			
		    				string = string + p[s].attrib_value[n2];
		    				string = string+"\t";
		    			
		    			
		    		}
		    		getdatabuffer.write(p[s].index+"\t"+string+p[s].cluster_id);
		    		
		    		getdatabuffer.write("\n");
		    		}
		    		else
		    		{
		    			// Output passed to reducer which contains
		    			// (Pt_index,partition_id+Cid+isCorePoint+eps_value+eps_diff+epsilon & kth dist ratio)
		    			String string=String.valueOf(p[s].attrib_value[0]);
			    		string = string+"\t";
			    		for(int n2=1;n2<p[s].attrib_value.length;n2++)
			    		{	
			    			
			    				string = string + p[s].attrib_value[n2];
			    				string = string+"\t";
			    			
			    			
			    		}
		    			
		    			
		    			if(p[s].cluster_id.equals("0") || p[s].cluster_id.equals("-1"))
		    			{
		    				contex.write(new IntWritable(p[s].index),new Text(String.valueOf(p[s].cluster_id+"\t"+false+"\t"+string)));
		    			}
		    			else
		    			{
		    			    			
		    			contex.write(new IntWritable(p[s].index),new Text(String.valueOf(p[s].cluster_id+"\t"+p[s].isCorepoint+"\t"+string)));
		    			
		    			
		    			
		    		}
		    		}
		    		
		    
		    		
		    		 
		    	
		    }
		    getdatabuffer.close();
		   // fs.close();
		    
		    
		    	
		    	
		    	
		    	
		
		
		   /* for(int s=0;s<val.size();s++)
		    {
    	
		    //contex.write(key,new Text(String.valueOf(p[s].index+"--->"+p[s].cluster_id)));
		    contex.write(key,new Text(String.valueOf(val.get(s));
    	
		    }*/
	
		
	
	
	
	}// end of mapper function
	
	
	
	
}// end of mapper class
