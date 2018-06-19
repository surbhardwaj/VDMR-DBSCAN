package Merge;

import java.awt.List;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.collections.SetUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.jasper.tagplugins.jstl.core.Set;



public class Compare_Merge 
{ 
	 public static void main (String [] args) throws Exception
	 {
	     try{
	           	ArrayList<Integer>key = new ArrayList<Integer>();							// Stores the key value
	           	ArrayList<String>record = new ArrayList<String>();
	           	ArrayList<ArrayList<String>>Merge = new ArrayList<ArrayList<String>>();		// stores the merge combinations corresponding to the key
	            ArrayList<ArrayList<String>>Merge_set = new ArrayList<ArrayList<String>>();	// stores the merge combinations which do not have any duplicates
	          
	            
	    	 	// Reading the Final output merged file from the HDFS
	    	 	BufferedReader br = new BufferedReader(new FileReader("/home/surbhi/Desktop/output.txt"));
	            String line;
	            line=br.readLine();
	            int i = 0;
	            while (line != null)
	            {	
	            	
	            	String []str = line.split("\t");
	            	//System.out.println(line);
	            	if(str.length!=1)
	            	{
	            	// inserting the key values
	            	key.add(Integer.valueOf(str[0]));
	            	//System.out.println(str.length);
	            	String string = "\t"+str[1]+"\t"+""+str[2];
	                record.add(string);
	            	
	            	// change the index value i.e 3 here according to the number of attributes
	                //System.out.println(str[3]);
	            	String st = StringUtils.substringBetween(str[3],"[","]");
	            
	            	//System.out.println(st);
	            	//stem.out.println(st);
	                String cluster[] = st.split(","); 
	               // System.out.println(cluster[0]);
	                Merge.add(new ArrayList<String>());
	                //Merge.get(i).addAll(cluster.);
	                
	                // adding the cluster values
	            	for(int j=0; j<cluster.length; j++)	
	                {
	                	Merge.get(i).add(cluster[j].trim());
	                	
	                }
	            	//System.out.println(Merge);
	                // Inserting the epsilon value for each cluster in Hash Map
	                
	                i++;
	                
	                line=br.readLine();
	            	}
	            }
	            
	             
	           
	            
/* Remove the duplicates from the ArrayList Merge-------------------------------------------------------------------------------*/
	           
	    	      for(int k=0; k<Merge.size(); k++)
	            {
	            	Collections.sort(Merge.get(k));
	            	//System.out.println(Merge.get(k));
	            	
	            }
	    	      
	            LinkedHashSet hs = new LinkedHashSet();
				hs.addAll(Merge);
				Merge_set.addAll(hs);    // Merge_set conatins non-duplicate merge combinations
				
				 //System.out.println(Merge_set); 
				
/*----------------------------------------------------------------------------------------------------------------------------*/				
				
				
				// Merging of the list 
				ArrayList<String>Merge_2 = new ArrayList<String>();	
				ArrayList<String>temp = new ArrayList<String>();	
				
				for(int l=0; l<Merge_set.size();l++)
				{
					for(int m=0; m<Merge_set.size();m++)
					{	
						//System.out.println(Merge_comb);
						//System.out.println(Merge_1.get(m));
						
						if(!ListUtils.intersection(Merge_set.get(l),(Merge_set.get(m))).isEmpty())
						{
							 Merge_2.addAll(ListUtils.intersection(Merge_set.get(l),(Merge_set.get(m))));	// common elements in both the list
							 if(Merge_2.size()==1)											// only one element should be common                   
							 {
								 double ep_dif = 0;
								 double max = 0;
								 temp.addAll(Merge_set.get(m));
								 temp.remove(Merge_2.get(0));
								 //Merge_1.get(m).remove(String.valueOf(Merge_2.get(0)));
								 	Merge_set.get(l).addAll(temp);
									 Merge_set.remove(m);			// remove the element at index m
									 m = m-1;
									 
								 
								 
							 }//end of if 
							 
							 
							 Merge_2.clear();
							 temp.clear();
						}// end of intersection if
						
						
						
						
					}// end of for loop
					
				}// end op outer for loop
				
				//System.out.println(Merge_comb);
				//System.out.println(Merge_1);
					
					//System.out.println(Merge_comb);
				//System.out.println(Merge_comb);
					//System.out.println(Merge_2);
					
				
				
				
				
				for(int k=0; k<Merge_set.size(); k++)		// sort by id before final merging
	            {
	            	Collections.sort(Merge_set.get(k));
	            	
	            }
				
				//System.out.println(find);
				//System.out.println(find_1);
				//System.out.println(Merge_1);
				System.out.println(Merge_set);
				
/*-----------------------------------------------------------------------------------------------------------------------------*/
				
				//	Renaming of clusters
				Writer output;
				output = new BufferedWriter(new FileWriter("/home/surbhi/Desktop/Merge_out.txt"));  //clears file every time
				
				//System.out.println(key.size());
				
				for(int p=0; p<key.size();p++)
				{	//System.out.println(Merge.get(p));
					
					for(int q=0; q<Merge_set.size();q++)
					{	
						
						
						if(!ListUtils.intersection(Merge.get(p),(Merge_set.get(q))).isEmpty())
						{
							 Merge_2.addAll(ListUtils.intersection(Merge.get(p),(Merge_set.get(q))));
							 if(Merge_2.size()>=2)
							 {
								 
								 output.append(""+key.get(p)+record.get(p)+"\t"+Merge_set.get(q).get(0));  // Renamed with first element of merged list
								 output.append("\n");
								 break;
								 
							 }
							 else if(Merge_2.size()==1)				// if clusters cannot be merged in that case.
							 {
								 if(Merge_set.get(q).size()==1)
								 {
									 output.append(""+key.get(p)+record.get(p)+"\t"+Merge_set.get(q).get(0));  // Renamed with first element of merged list
									 output.append("\n");
									 break;
									 
									 
								 }
								 
							 }
						}
						Merge_2.clear();
						
					}
					
					Merge_2.clear();
				}
					output.close();
/*-----------------------------------------------------------------------------------------------------------------------------*/
					
					// Global Relabelling of all the data points present in the dataset
					
					/*BufferedReader br1 = new BufferedReader(new FileReader("/home/surbhi/Desktop/Global.txt"));
					FileWriter fw = new FileWriter("/home/surbhi/Desktop/Global_Final.txt");  
				    PrintWriter out = new PrintWriter(fw);
		            String line1;
		            line1 = br1.readLine();
		            int j = 0;
		            int k=0;
		            while (line1 != null)
		            {	
		            	
		            	String []str = line1.split("\t");
		            	for(k=0; k<Merge_comb.size();k++)
		            	{	
		            		
		            		if(Merge_comb.get(k).contains(str[1]))
		            		{
		            			// if the cluster-id is present in merged list
		            			int index= Merge_comb.get(k).indexOf(str[1]);
		            			if(index >=1)											// element itself is not the first element or representative element of the merged list
		            			{
		            				line1=line1.replace(str[1], Merge_comb.get(k).get(0));   // Replace it with the first element of the merged list
		            				out.write(line1);
		            						
		            			}
		            			out.write(line1);
		            			break; // move out of the loop if the cluster-id is present in any of the merged list
		            			
		            		}
		            		
		            	}
		            	if(k== Merge_comb.size())
		            		out.write(line1);
		            	
		            	
		            	
		            }
							
						//out.close();	*/
						
		 }catch(Exception e)
	        {
	        	System.out.println(e);
	        }
	 }
	
	
}

