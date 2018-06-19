package dbscan;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<IntWritable, Text>
{

	

	@Override
	public int getPartition(IntWritable Key, Text value, int numReduceTasks) 
	{
		
		 if(Key.toString().contains("-1"))
         {
             return 0;
         }else if(Key.toString().contains("-2"))
         {
             return 1;
         }
         else
         {
        	 return (Key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        	 
        	 
        	 
         }
		
	}
	
	
	

}
