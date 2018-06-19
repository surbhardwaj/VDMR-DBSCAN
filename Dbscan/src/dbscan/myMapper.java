package dbscan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import kd_package.KDTree;
import kd_package.KeyDuplicateException;
import kd_package.KeySizeException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




public class myMapper extends Mapper<NullWritable, Text, NullWritable, Text> 
{
 
 
	@SuppressWarnings("unchecked")
	public void map(NullWritable key, Text value,Context contex) throws IOException, InterruptedException 
	{
	 	int len=0;
	 	String lines = value.toString();
	    String []lineArr = lines.split("\n"); 													// LineArr contains the array of records in the dataset separated by tab.
	    len = lineArr.length;								
		
		@SuppressWarnings("unused")
		MyComparator comparator = new MyComparator();										   // For sorting the ArrayList value based on the distance value.
		Point []p;																								// Array of point objects
		KDTree<Integer> kdtree = new KDTree<Integer>(8);																			// 3 is the dimensions of the dataset
	    
	    	
		p=new Point[len];
		HashMap<Integer,Double>KD_Dist = new HashMap<Integer, Double>();
		
		for(int i=0;i<len;i++)
		{
			
			p[i]=new Point(lineArr[i]);
			
		}
		
		
		
		/* Create KDtree for the data points **/
		
		
		for(int i = 0; i < len ; i++) 
		{
			try {
				
				
				try {
					kdtree.insert(p[i].toDouble(), p[i].index);
				} catch (KeySizeException | KeyDuplicateException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch(Exception e) {}
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
				List<Integer> nearestNeighbours = kdtree.nearest(p[j].toDouble(), n+1);		// if you need to find nth nearest neighboue keep the value of n=n+1
				Collections.reverse(nearestNeighbours);
				
				int nearestPointIndex = nearestNeighbours.get(n);			// It also returns number itself as the nearest neighbour and they are returned as stack list
				//System.out.println(nearestPointIndex);								// The numbers are arranged in descending order
				
				sum=0;
				K_distance=0;
				for(int i=0;i<leng;i++)
				{
					sum=sum+Math.pow((p[j].attrib_value[i]-p[nearestPointIndex-1].attrib_value[i]), 2);
					
					
				}
				
					K_distance= Math.sqrt(sum);
					KD_Dist.put(j+1, K_distance);
				
			}
			catch(Exception | KeySizeException e){System.out.println(e);}
			
			
		}
		
		
				
				/*Sorting of the KNN Hash map based on K nearest neighbours distance*/
				
				@SuppressWarnings({ "rawtypes" })
				ArrayList mapKeys = new ArrayList(KD_Dist.keySet());
				@SuppressWarnings({ "rawtypes" })
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
				        Object keys = keyIt.next();
				        String comp1 = KD_Dist.get(keys).toString();
				        String comp2 = val.toString();

				        if (comp1.equals(comp2))
				        {
				            KD_Dist.remove(keys);
				            mapKeys.remove(keys);
				            sortedMap.put((Integer)keys, (Double)val);
				            break;
				        }
				    }
				}
				
				// Sorted Map contains the Kdist values in sorted order
		
				/* Fetching the values of keys and KDist values in ascending order in some ArrayList*/
				
				ArrayList<Integer> keys=new ArrayList<Integer>();
				ArrayList<Double> Kdist=new ArrayList<Double>();
				keys.addAll(sortedMap.keySet());								// Arraylist of keys in sorted order
				Kdist.addAll(sortedMap.values());								// Arraylist of values in sorted order
				
				
				
				
				
				
				
				
				/* Creation of Hash Map for Density Variation list */
				
				LinkedHashMap<String,Double> DenVar = new LinkedHashMap<String,Double>();
				for(int i=1;i<len;i++)
				{
					
					DenVar.put(keys.get(i-1)+"\t"+keys.get(i), ((Kdist.get(i)-Kdist.get(i-1))/Kdist.get(i-1)));   // Denvar hashmap contains the Denvarlist
					
					
				}
				
				ArrayList<Double>Den_list=new ArrayList<Double>();
				Den_list.addAll(DenVar.values());
		
		//System.out.println(DenVar);
		
		
		/* Calculation of the threshold value  th*/
		
		
	    /* Calculation of Mathematical Expectation */
		/*double sum1 = 0;
		double omega=2.5;														// Ideal value of omega is 2.5 but varies between (0,3]
		double th=0;
	    int n1 = Den_list.size();
	    double EX=0;
	    																		// Iterating manually is faster than using an enhanced for loop.
	    for (int i = 0; i < n1; i++)
	        sum1 += Den_list.get(i);
	    
	     EX=((double) sum1) /n1;        
	    
	    
	 /* Calculation od SD for the Den_list */ 
	    /*sum1=0;
	    double dev = 0;
	    for (int i = 0; i < n1; i++)
	    {  
            sum1 += Math.pow((Den_list.get(i) - EX), 2);  
  
        }  
	    dev = Math.sqrt(sum1 / (n1));
	    
	    
	    // Threshol value calculations
	    
	    th = EX + (omega*dev);
	    //th=0.08;
	    //System.out.println(th);
	    
	    
	    
	    /* Dividing the points in various density level sets. */
	    
	    /*LinkedHashMap<Integer, ArrayList<Integer>> DLS=new LinkedHashMap<Integer, ArrayList<Integer>>();
	    int i=1;
	    ArrayList<String>Den_keys=new ArrayList<String>();
		Den_keys.addAll(DenVar.keySet());	
	    ArrayList<ArrayList<Integer>>sets=new ArrayList<ArrayList<Integer>>();
	    sets.add(new ArrayList<Integer>());
	    int j=1;
	    
	    for ( double val: DenVar.values()) 
	    {	
	    	
	    	//zSystem.out.println(Den_keys.get(i-1));
	    	String []var=new String[2];
	    	var=(Den_keys.get(i-1)).split("\t");
	    	if(i==1)
	    	{
	    	 if(val<=th)
	    	 {
	    		
	    		 sets.add(new ArrayList<Integer>());
	    		sets.get(j-1).add(Integer.valueOf(var[0]));
	    		sets.get(j-1).add(Integer.valueOf(var[1]));
	    		
	    	 }
	    	if(val>th)
	    	{
	    		
	    		
	    		sets.get(j-1).add(Integer.valueOf(var[0]));
	    		DLS.put(j, sets.get(j-1));																	// Removes the items from the list
	    		j++;
	    		sets.add(new ArrayList<Integer>());
	    		sets.get(j-1).add(Integer.valueOf(var[1]));
	    		
	    		
	    		
	    		
	    	}
	     }
	    	
	    	else 
	    	{
	    		if ( val > th)																// If the value is found to be greater than threshold  
	    		
	    		{
	    			
	    			
	    			
		    		
		    		DLS.put(j, sets.get(j-1));																	// Removes the items from the list
		    		j++;
		    		sets.add(new ArrayList<Integer>());
		    		sets.get(j-1).add(Integer.valueOf(var[1]));
	    		
	        	
	        	
	    		}
	    	  else
	    	  	{
	    		sets.add(new ArrayList<Integer>());
	    		sets.get(j-1).add(Integer.valueOf(var[1]));
	    		
	    	  	}
	    	}
	        
	        i++;
	        
	    }
		
	    DLS.put(j, sets.get(j-1));							// DLS is the hash map for the density level sets.*/
	    
	    
	    
		
		
		
		
		
		
		for(int s=0;s<len;s++)
	    	{
	    	
	    		contex.write(key,new Text(lineArr[s]));
	    	
	    	}
	}
	    
	    
 }
