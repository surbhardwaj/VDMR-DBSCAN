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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.jasper.tagplugins.jstl.core.Set;

public class Final_Merge 
{ 
	 public static void main (String [] args) throws Exception
	 {
	     try{
	           	ArrayList<Integer>key = new ArrayList<Integer>();							// Stores the key value
	           	ArrayList<String>record = new ArrayList<String>();
	           	ArrayList<ArrayList<String>>Merge = new ArrayList<ArrayList<String>>();		// stores the merge combinations corresponding to the key
	            ArrayList<ArrayList<String>>Merge_set = new ArrayList<ArrayList<String>>();	// stores the merge combinations which do not have any duplicates
	            LinkedHashMap<String, Double> epsilon = new LinkedHashMap<String, Double>();
	            double thresh = (1.8871949374160983+0.7937121338349264+1.864516463163823+2.269445237268947)/4.0; 	
	            		//( 1.6058455062781114+1.6704717139065335+0.375954538571775+0.4822468916636562)/4.0;			
	            //(0.8197098163274144 + 1.63855493306089 + 1.4891024888911404 + 0.41073399787195375)/4.0;
	    	 	
	    	 	// Reading the Final output merged file from the HDFS
	    	 	BufferedReader br = new BufferedReader(new FileReader("/home/surbhi/Desktop/output1.txt"));
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
	            	
	                String cluster[] = str[3].split("-");          
	                Merge.add(new ArrayList<String>());
	                
	                // adding the cluster values
	            	for(int j=0; j<cluster.length; j++)	
	                {
	                	Merge.get(i).add(cluster[j]);
	                	
	                }
	            	//System.out.println(Merge);
	                // Inserting the epsilon value for each cluster in Hash Map
	                epsilon.put(cluster[0], Double.valueOf(str[4]));
	                epsilon.put(cluster[1], Double.valueOf(str[5]));
	                i++;
	                
	                line=br.readLine();
	            	}
	            }
	            
	            
	           // System.out.println(Merge);
	            
/* Remove the duplicates from the ArrayList Merge-------------------------------------------------------------------------------*/
	           
	    	      for(int k=0; k<Merge.size(); k++)
	            {
	            	Collections.sort(Merge.get(k));
	            	
	            }
	    	      System.out.println(Merge);
	            LinkedHashSet hs = new LinkedHashSet();
				hs.addAll(Merge);
				Merge_set.addAll(hs);    // Merge_set conatins non-duplicate merge combinations
				//System.out.println(Merge_set);
				ArrayList<Double>eps_diff = new ArrayList<Double>();		// Difference in the epsilon values of two merge combinations
				// Merge set is in combination of two initially
				for(int k=0; k<Merge_set.size();k++)
				{
					double diff = Math.abs(epsilon.get(Merge_set.get(k).get(0)) - epsilon.get(Merge_set.get(k).get(1)));
					eps_diff.add(diff);
					
				}
				//System.out.println(Merge_set);
				//System.out.println(eps_diff);
/*----------------------------------------------------------------------------------------------------------------------------*/				
				
				// Merging the sets based on eps value difference
				
				int counter = 0;
				ArrayList<ArrayList<String>>Merge_comb = new ArrayList<ArrayList<String>>();		// stores the merged combinations
				ArrayList<ArrayList<String>>Merge_1 = new ArrayList<ArrayList<String>>();		// stores the merged combinations
				
				for(int j=0; j<=eps_diff.size(); j++)
				{
					int minIndex = eps_diff.indexOf(Collections.min(eps_diff));		// minimum epsilon difference value index, is merged first
					//System.out.println(minIndex);
					if(counter == 0)
					{
						Merge_comb.add(Merge_set.get(minIndex));
						Merge_set.remove(minIndex);
						eps_diff.remove(minIndex);
						j=0;
					}
					else
					{	
						int k=0;
						for(k=0; k<Merge_comb.size();k++)
						{	
								
							//Set<Integer> intersection = Sets.intersection(Sets.newHashSet(listA), Sets.newHashSet(listB));
								if(ListUtils.intersection(Merge_set.get(minIndex),(Merge_comb.get(k))).isEmpty())
								{	

									continue;
									
								}
								else
									break;
								
						}
						if(k== Merge_comb.size())			// No intersection is found
						{
							
							Merge_comb.add(Merge_set.get(minIndex));
							Merge_set.remove(minIndex);
							eps_diff.remove(minIndex);
							
							
						}								// intersection is found in the values
						else
						{
							Merge_1.add(Merge_set.get(minIndex));
							Merge_set.remove(minIndex);
							eps_diff.remove(minIndex);
							
						}
						
						
					}
					counter ++;
					j=0;
					
				}
				
				//System.out.println(Merge);
				
				//System.out.println(Merge_1);
				//System.out.println(Merge_comb);
				// Merge_1 arraylist contains the combinations which are to be merged with Merge_comb list
				
/*-----------------------------------------------------------------------------------------------------------------------------*/
				// Final Merging of the list 
				ArrayList<String>Merge_2 = new ArrayList<String>();	
				ArrayList<String>temp = new ArrayList<String>();	
				
				for(int l=0; l<Merge_comb.size();l++)
				{
					for(int m=0; m<Merge_1.size();m++)
					{	
						//System.out.println(Merge_comb);
						//System.out.println(Merge_1.get(m));
						
						if(!ListUtils.intersection(Merge_comb.get(l),(Merge_1.get(m))).isEmpty())
						{
							 Merge_2.addAll(ListUtils.intersection(Merge_comb.get(l),(Merge_1.get(m))));	// common elements in both the list
							 if(Merge_2.size()==1)											// only one element should be common                   
							 {
								 double ep_dif = 0;
								 double max = 0;
								 temp.addAll(Merge_1.get(m));
								 temp.remove(Merge_2.get(0));
								 //Merge_1.get(m).remove(String.valueOf(Merge_2.get(0)));
								 max = Math.abs(epsilon.get(Merge_comb.get(l).get(0))-epsilon.get(temp.get(0)));
								 for(int n=1; n<Merge_comb.get(l).size();n++)
								 {
									 ep_dif = Math.abs(epsilon.get(Merge_comb.get(l).get(n))-epsilon.get(temp.get(0)));
									 if(ep_dif>max)
									 {
										 max= ep_dif;
									 }
									 
								 }
								 if(max<= thresh)
								 {
									Merge_comb.get(l).add(temp.get(0));
									 Merge_1.remove(m);			// remove the element at index m
									 m = m-1;
									 
								 }
								 
							 }//end of if 
							 else
								 Merge_1.remove(m);				// if more than one element is common then remove it
								 
							 
							 Merge_2.clear();
							 temp.clear();
						}// end of intersection if
						
						
						
						
					}// end of for loop
					
				}// end op outer for loop
				
				//System.out.println(Merge_comb);
				//System.out.println(Merge_1);
				ArrayList<ArrayList<String>>find = new ArrayList<ArrayList<String>>();
				ArrayList<ArrayList<String>>find_1 = new ArrayList<ArrayList<String>>();
				
				// inserting the remaining elements of the list which cannot be merged
				//System.out.println(Merge_1.size());
				int size = Merge_comb.size();
			//	Merge_comb.add(new ArrayList<String>());
				
					Merge_comb.addAll(Merge_1);
					
					//System.out.println(Merge_comb);
				//System.out.println(Merge_comb);
				int ij=0;
				for(int k=0; k<Merge_comb.size();k++)
				{
					//System.out.println(Merge_comb);
					
					for(int l=k+1; l<Merge_comb.size();l++)
					{
						
						if(!ListUtils.intersection(Merge_comb.get(k),(Merge_comb.get(l))).isEmpty())
						{	
							
							 Merge_2.addAll(ListUtils.intersection(Merge_comb.get(k),(Merge_comb.get(l))));	// common elements in both the list
							 
							 if(Merge_2.size()==1)											// only one element should be common                   
							 {
								 //System.out.println(Merge_comb.get(k));
								 if(Merge_comb.get(k).size()<2)
								 {
									 Merge_comb.remove(k);
									 k=k-1;
								 }
								 else if(Merge_comb.get(l).size()<2)
								 {
									 Merge_comb.remove(l);
									 l=l-1;
									 
								 }
								 else
								 {
								 double ep_dif = 0;
								 double max = 0;
								 temp.addAll(Merge_comb.get(l));
								 temp.remove(Merge_2.get(0));  // removing the intersection
								 //Merge_1.get(m).remove(String.valueOf(Merge_2.get(0)));
								 max = Math.abs(epsilon.get(Merge_comb.get(k).get(0))-epsilon.get(temp.get(0)));
								 for(int n=1; n<Merge_comb.get(k).size();n++)
								 {	
									 ep_dif = Math.abs(epsilon.get(Merge_comb.get(k).get(n))-epsilon.get(temp.get(0)));
									 if(ep_dif>max)
									 {
										 max= ep_dif;
									 }
									 
								 }
								if(max<= thresh)
								 {
									
									Merge_comb.get(k).add(temp.get(0));
									 Merge_comb.remove(l);			// remove the element at index m
									 l = l-1;
									 
								 }
								 else
								 {	
									 int ind=0;
									 //System.out.println(Merge_2);
									 if(find.contains(Merge_comb.get(l)))
									 {
										 ind = find.indexOf(Merge_comb.get(l));
										
										 find_1.get(ind).add(Merge_2.get(0));
										 
									 }
									 else
									 {
									 
									 find.add(new ArrayList<String>());
									 find_1.add(new ArrayList<String>());
									 
									 find.get(ij).addAll(Merge_comb.get(l));
									 find_1.get(ij).add(Merge_2.get(0));
									 //Merge_comb.get(l).remove(Merge_2.get(0));       // if cannot be merged
									// l=l-1;
									 ij=ij+1;
									 }
								 }
							 }
							 
							 Merge_2.clear();
							 temp.clear();
							 }
						
					}
					
					}
					
					
					
					//System.out.println(Merge_2);
					
				}
				
				
				for(int p=0;p<find.size();p++)
				{
					if(Merge_comb.contains(find.get(p)))
					{	
						 int in = Merge_comb.indexOf(find.get(p));
						if(!ListUtils.intersection(find.get(p),(find_1.get(p))).isEmpty())
						{	
							
							 Merge_2.addAll(ListUtils.intersection(find.get(p),(find_1.get(p))));
							 if(Merge_2.size()==find.get(p).size())
							 {
								
								 Merge_comb.remove(in);
							 }
							 else
							 {
								 
								 for(int n=0;n<Merge_2.size();n++)
								 {
									 Merge_comb.get(in).remove(Merge_2.get(n));
									 
								 }
							 }
						}
						Merge_2.clear();
						
					}
					
				}
				for(int k=0; k<Merge_comb.size(); k++)		// sort by id before final merging
	            {
	            	Collections.sort(Merge_comb.get(k));
	            	
	            }
				System.out.println(Merge_comb);
				//System.out.println(find);
				//System.out.println(find_1);
				//System.out.println(Merge_1);
				//System.out.println(Merge);
				
/*-----------------------------------------------------------------------------------------------------------------------------*/
				
				//	Renaming of clusters
				Writer output;
				output = new BufferedWriter(new FileWriter("/home/surbhi/Desktop/Merge_out.txt"));  //clears file every time
				
				//System.out.println(key.size());
				
				for(int p=0; p<key.size();p++)
				{	//System.out.println(Merge.get(p));
					
					for(int q=0; q<Merge_comb.size();q++)
					{	
						
						
						if(!ListUtils.intersection(Merge.get(p),(Merge_comb.get(q))).isEmpty())
						{
							 Merge_2.addAll(ListUtils.intersection(Merge.get(p),(Merge_comb.get(q))));
							 if(Merge_2.size()>=2)
							 {
								 
								 output.append(""+key.get(p)+record.get(p)+"\t"+Merge_comb.get(q).get(0));  // Renamed with first element of merged list
								 output.append("\n");
								 break;
								 
							 }
							 else if(Merge_2.size()==1)				// if clusters cannot be merged in that case.
							 {
								 if(Merge_comb.get(q).size()==1)
								 {
									 output.append(""+key.get(p)+record.get(p)+"\t"+Merge_comb.get(q).get(0));  // Renamed with first element of merged list
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

