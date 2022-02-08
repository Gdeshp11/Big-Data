package Reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;

public class Reduce 
    extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
    
    
    
    
    }
    
}
