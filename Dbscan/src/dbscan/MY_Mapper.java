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

public class MY_Mapper extends Mapper<NullWritable, Text, IntWritable, Text> 
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
		KDTree kdtree = new KDTree(2);							// 2 is the dimensions of the dataset
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
		
		
		/* Finding K-Dist for each point the dataset and storing it in a hash map */
		
		int n=4;											
		int leng = p[0].attrib_value.length;
		double sum = 0;
		double K_distance = 0;
		for(int j=0;j<len;j++)
		{
			try
			{
				List nearestNeighbours = kdtree.nearest(p[j].toDouble(), n+1);		// if you need to find nth nearest neighboue keep the value of n=n+1
				Collections.reverse(nearestNeighbours);
				
				int nearestPointIndex = (Integer)nearestNeighbours.get(n);			// It also returns number itself as the nearest neighbour and they are returned as stack list
				// The numbers are arranged in descending order
				sum=0;
				K_distance=0;
				for(int i=0;i<leng;i++)
				{
					sum=sum+Math.pow((p[j].attrib_value[i]-p[nearestPointIndex-1].attrib_value[i]), 2);
					
					
				}
				
				K_distance= Math.sqrt(sum);
				if(p[j].isCommon_point == true)
					p[j].boundary_kdist = K_distance;
				KD_Dist.put(j+1, K_distance);
				
			}
			catch(Exception | KeySizeException e){System.out.println(e);}
			
			
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
	    
	    
/*-----------------------------------------------------------------------------------------------------------------------------*/
		/*Sorting of the KNN Hash map based on K nearest neighbours distance*/
		
		@SuppressWarnings("rawtypes")
		ArrayList mapKeys = new ArrayList(KD_Dist.keySet());
		@SuppressWarnings("rawtypes")
		ArrayList mapValues = new ArrayList(KD_Dist.values());
		Collections.sort(mapValues);
		Collections.sort(mapKeys);
		@SuppressWarnings("rawtypes")
		LinkedHashMap sortedMap = new LinkedHashMap();
		@SuppressWarnings("rawtypes")
		Iterator valueIt = mapValues.iterator();
		while (valueIt.hasNext()) 
		{
		    Object val = valueIt.next();
		    @SuppressWarnings("rawtypes")
			Iterator keyIt = mapKeys.iterator();
		    while (keyIt.hasNext()) 
		    {
		        Object key1 = keyIt.next();
		        String comp1 = KD_Dist.get(key1).toString();
		        String comp2 = val.toString();

		        if (comp1.equals(comp2))
		        {
		            KD_Dist.remove(key1);
		            mapKeys.remove(key1);
		            sortedMap.put((Integer)key1, (Double)val);
		            break;
		        }
		    }
		}
		
		
		/* Fetching the values of keys and KDist values in ascending order in some ArrayList*/
		
		ArrayList<Integer> keys=new ArrayList<Integer>();
		ArrayList<Double> Kdist=new ArrayList<Double>();
		keys.addAll(sortedMap.keySet());								// Arraylist of keys in sorted order
		Kdist.addAll(sortedMap.values());								// Arraylist of values in sorted order
		
		
		
/*-----------------------------------------------------------------------------------------------------------------------------*/
		
		
		
		
		
		/* Creation of Hash Map for Density Variation list */
		
		LinkedHashMap<String,Double> DenVar = new LinkedHashMap<String,Double>();
		for(int i=1;i<len;i++)
		{
			
			DenVar.put(keys.get(i-1)+"\t"+keys.get(i), ((Kdist.get(i)-Kdist.get(i-1))/Kdist.get(i-1)));   // Denvar hashmap contains the Denvarlist
			
		}
		
		ArrayList<Double>Den_list=new ArrayList<Double>();
		Den_list.addAll(DenVar.values());									// Den_list contains all the Density var list values.
		
		
		/* Calculation of Mathematical Expectation */
		double sum1 = 0;
		double omega=2.5;													// Ideal value of omega is 2.5 but varies between (0,3]
		double th=0;
	    int n1 = Den_list.size();
	    double EX=0;
	    																	// Iterating manually is faster than using an enhanced for loop.
	    for (int i = 0; i < n1; i++)
	        sum1 += Den_list.get(i);
	    
	    
	    EX=((double) sum1) /n1;
	    
	    
	    	/* Calculation od SD for the Den_list */ 
	     
	    
	    sum1=0;
	    double dev = 0;
	    for (int i = 0; i < n1; i++)
	    {  
            sum1 += Math.pow((Den_list.get(i) - EX), 2);  
  
        }  
	    dev = Math.sqrt(sum1 / (n1));
	    
	    
	    // Threshold value calculations
	    
	    th = EX + (omega*dev);
	   
	   
	    
/*--------------------------------------------------------------------------------------------------------------------------*/
	    
	    
	   /* Dividing the points in various density level sets. */
	    
	   
	    int i=1;
	    ArrayList<String>Den_keys=new ArrayList<String>();
		Den_keys.addAll(DenVar.keySet());	
	    ArrayList<ArrayList<Integer>>sets=new ArrayList<ArrayList<Integer>>();
	    sets.add(new ArrayList<Integer>());
	    int j=1;
	    
	    for ( double values: DenVar.values()) 
	    {	
	    	
	    	
	    	String []var=new String[2];
	    	var=(Den_keys.get(i-1)).split("\t");
	    	if(i==1)
	    	{
	    		if(values<=th)
	    		{
	    			sets.get(j-1).add(Integer.valueOf(var[0]));
	    			sets.get(j-1).add(Integer.valueOf(var[1]));
	    		
	    		}
	    		if(values>th)
	    		{
	    			sets.get(j-1).add(Integer.valueOf(var[0]));
	    			j++;
	    			sets.add(new ArrayList<Integer>());
	    			sets.get(j-1).add(Integer.valueOf(var[1]));
	    		}
	    	}
	    	else 
	    	{
	    		if ( values > th)																// If the value is found to be greater than threshold  
	    		{
	    			j++;
		    		sets.add(new ArrayList<Integer>());
		    		sets.get(j-1).add(Integer.valueOf(var[1]));
	    		}
	    		else
	    		{
	    		
	    			sets.get(j-1).add(Integer.valueOf(var[1]));
	    		
	    		}
	    	}
	        
	        i++;
	        
	    }
	    
	    
 /*--------------------------------------------------------------------------------------------------------------------------*/
	    
	    
	    
	    /*  Refinement of the DLS */
	    
		   // 1.	Removal Process
		   
		    
		    int num = sets.size();
		    double set_size=0;
		    double scatter=0;
		    double sd=0;
		    double meankd=0;
		    double tot=0;
		    double th_size = ((6/1000)*len); //0.6 %
		    ArrayList<Double>mean=new ArrayList<Double>();											// Storing the meankd value of the sets obtained after step-1
		    for( int m=0; m<num; m++)
		    {
		    	
		    	set_size=sets.get(m).size();
		    	tot=0;
		    	for(int x=1;x<=set_size;x++)
		    	{
		    		tot=tot+Double.valueOf(((sortedMap.get(sets.get(m).get(x-1))).toString()));									// Finding the mean of KDIST values of the density level sets.
		    		
		    	}
		    	meankd=(tot/set_size);
		    	tot=0;
		    	for(int x=1;x<=set_size;x++)
		    	{
		    		tot += Math.pow((Double.valueOf(((sortedMap.get(sets.get(m).get(x-1))).toString())) - meankd), 2);  			// Finding standard deviation of DLS
		    		  
		        }  
			    sd = Math.sqrt(tot / set_size);
			    //System.out.println("sd value is"+sd);
			    scatter=(1/Math.sqrt(set_size))*(sd/meankd);				// Calculating scatter value of each DLS 
			    //System.out.println("scatter value is"+scatter);
		    	if(set_size<th_size || scatter>0.12)	//0.12
		    	{
		    		sets.set(m,null);
		    		
		    	}
		    	else
		    	{
		    		mean.add(meankd);
		    		
		    	}
		    	
		    	
		    }
		    
		    sets.removeAll(Collections.singleton(null));				// Remove the null elements from the set and form a refined set 
		    															// which is further passed for merging phase
		    
		    
			  
		    
		    
		   // Merging of the sets if they have Density gradient value is less that 0.2
		    
		    int size=sets.size();										// Updated size of the DLS sets.
		    meankd=0;
		    double mean_kd=0;
		    double new_mean=0;
		    double large=0,small=0;
		    double dengrad=0;
		    int m1,m2=0;
		    for(int y=0;y<size;y++)
		    {
		    	for(int a=y+1;a<size;a++)
		    	{
		    		
		    		meankd=mean.get(y);
		    		mean_kd=mean.get(a);
		    		if(meankd>mean_kd)
		    		{
		    			large=meankd;
		    			small=mean_kd;
		    		}
		    		else
		    		{
		    			large=mean_kd;
		    			small=meankd;
		    		}
		    			
		    		
		    		dengrad=((large/small)-1);
		    		//System.out.println("Value of dengrad is"+dengrad);	
		    		if(dengrad<0.2)	// 0.02, 0.2
		    		{
		    			sets.get(y).addAll(sets.get(a));
		    			m1=sets.get(y).size();
		    			m2=sets.get(a).size();
		    			new_mean=(((meankd*m1)+(mean_kd*m2))/(m1+m2));							// updated KDistance mean value without accessing the sortedMap again.
		    			mean.set(y,new_mean);
		    			sets.remove(a);
		    			mean.remove(a);
		    			a=a-1;
		    			size=sets.size();
		    			
		    		}
		    	}
		    }
		    
		   // Final sets are obtained which are refined. 	
		    
		   		
/*-----------------------------------------------------------------------------------------------------------------------------*/		    		
		   
		    
		    
		    /*  Calculation of different eps values */
		    
		    ArrayList<Double> eps = new ArrayList<Double>();							// Arraylist of epsilon values for different DLS
		    double mean_value=0;
		    ArrayList<Double>Kd_list = new ArrayList<Double>();
		    @SuppressWarnings("unused")
			double kd_value;
		    double median_value = 0;
		    int middle=0;
		    double max_kd = 0;
		    double eps_value = 0;
		   
		    
		    for(int c=1;c<=sets.size();c++)
		    {	
		    	
		    	mean_value = mean.get(c-1);
		    	Kd_list = new ArrayList<Double>();
		    	for(int d=1;d<=sets.get(c-1).size();d++)
		    	{
		    		Kd_list.add(Double.valueOf(((sortedMap.get(sets.get(c-1).get(d-1))).toString())));	
		    		
		    		
		    	}
		    	double total=0;
		    	for(int m=0;m<Kd_list.size();m++)
		    	{
		    		
		    		total=total+Kd_list.get(m);
		    		
		    	}
		    		mean_value = total/Kd_list.size();
		    	
		    	 middle = (Kd_list.size())/2;
		    	 if (Kd_list.size()%2 == 1) 
		    	 {
		    		 median_value = Kd_list.get(middle);
		    	 } 
		    	 else 
		    	 {
		    	     median_value = (Kd_list.get(middle-1) + Kd_list.get(middle)) / 2.0;
		    	 }
		    	   
		    	  max_kd = Collections.max(Kd_list);
		    	  eps_value = (max_kd)*(Math.sqrt((median_value/mean_value)));
		    	  eps.add(eps_value); 
		    	  Kd_list.clear();
		    	    
		    }
		    
		    	
		    	//contex.write(new IntWritable(-2),new Text(String.valueOf(mini)));
		    
/*---------------------------------------------------------------------------------------------------------------------------*/		    
		     
		    
		    /* Applying DBSCAN on the eps values and data points obtained */
		    
		    ArrayList<Integer>val = new ArrayList<Integer>();
		    ArrayList<Double> used_eps = new ArrayList<Double>();
		    int to=0;
		    
		    // Variable declaration
		    double epsilon = 0;
		    int min_points = n;
		    Point current_p, result_p;
		    int cid	= 0;					// counter for the cluster id
		    
		    
		    
		    // DBSCAN code
		    // p[i].cluster contains the integer part of cluster id
		    // p[i].cluster_id contains the cluster id with partition
		   // int flag = 0;
		    //p[1].flag=1;
		    for(int k=0;k<sets.size();k++)
		    {
		    	epsilon = eps.get(k);					// epsilon value for a particular set
		    	
		    	for(int l=0;l<sets.get(k).size();l++)
		    	{	
		    		
		    		int pt_index = sets.get(k).get(l);
		    		
		    		current_p = p[pt_index-1];
		    		
		    		if (current_p.cluster==0)		// cluster_id is -1 for noise, 0 for unclassified and other values are for cluster id
		    		{
		    			// Cluster expansion part
		    			
		    			
		    			try
						{
							// Finding points in the epsilon neighbourhood of a point
							List<Integer>seeds = kdtree.nearestEuclidean(current_p.toDouble(), epsilon);
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
								current_p.eps_value = (Double) null;
								current_p.isCorepoint = false;
								continue;
								
							}
							else
							{
								// if point is a core point
								// all point in thye seed are density reachable
								// from the point
								to=0;
								cid = cid+1;
								if(!used_eps.contains(epsilon))
								{
									used_eps.add(epsilon);		// list of epsilon values used in DBSCAN
								}
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
								
								seeds.remove((Integer)pt_index);			// it removes the object with given value from the list
								
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
		 }
		 // End of DBSCAN part 
	
/*-----------------------------------------------------------------------------------------------------------------------------*/
		    
		    // calculation of minimum of the used epsilon in the partition
		    
		   double mini = 0.0;
		    if(used_eps.size()>1)
		    {
	    	mini = Math.abs(used_eps.get(0)-used_eps.get(1));
	    	
	    	for(int m=1; m<used_eps.size();m++)						// min difference of epsilon in the partition 
	    	{
	    		for(int q=m+1; q<used_eps.size();q++)
	    		{	
	    			double cal = Math.abs(used_eps.get(m)-used_eps.get(q));
	    			if(cal<mini)
	    			{
	    				
	    				mini = cal;
	    			}
	    			
	    		}
	    		
	    		
	    	}
		    }
		    else
		    	mini = used_eps.get(0);
	    
	    	//contex.write(new IntWritable(-1),new Text(String.valueOf(mini)));
	       // getdatabuffer.write("-1"+","+mini);
	    	// getdatabuffer.write("\n");
		    
		  
		    
	    
	    
/*-----------------------------------------------------------------------------------------------------------------------------*/	    
	    
		
		   /* Output writing part */
		    
		    /*Configuration conf = new Configuration();							// writing output to HDFS
		    FileSystem fs = FileSystem.get(conf);
		    Path inputfile = new Path("in/map");
		    BufferedWriter getdatabuffer = new BufferedWriter(new OutputStreamWriter(fs.create(inputfile)));*/
		    
		    int l=eps.size();
		    double val1,val2;
		    int index=0;
		    double ratio = 0;
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
		    			index=eps.indexOf(p[s].eps_value);
			    		val1=0;
			    		val2=0;
		    			double min=0;
		    			if(index-1>=0)
		    			{
		    				
		    			val1 = Math.abs(p[s].eps_value-eps.get(index-1));
		    			}
		    			if(index+1<=l-1)
		    			{
		    			val2 =  Math.abs(p[s].eps_value-eps.get(index+1));
		    			}
		    			if(val1==0)
		    				min=val2;
		    			else if(val2==0)
		    				min=val1;
		    			else 
		    			{
		    				if(val1<val2)
		    					min=val1;
		    				else
		    					min=val2;
		    			}
		    			ratio = (p[s].eps_value/p[s].boundary_kdist);	// epsilon / k_dist ratio for the boundary points which is later used for cluster_id assignment
		    			
		    			contex.write(new IntWritable(p[s].index),new Text(String.valueOf(p[s].cluster_id+"\t"+p[s].isCorepoint+"\t"+p[s].eps_value+"\t"+min+"\t"+ratio+"\t"+used_eps+"\t"+string)));
		    			
		    			
		    			
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
