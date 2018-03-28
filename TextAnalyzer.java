import org.apache.hadoop.fs.Path;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.HashSet;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Do not change the signature of this class
public class TextAnalyzer extends Configured implements Tool {
	
    // Replace "?" with your own output key / value types
    // The four template data types are:
    //     <Input Key Type, Input Value Type, Output Key Type, Output Value Type>
    public static class TextMapper extends Mapper<LongWritable, Text, Text, QueryPair> {
    	private Text context_word = new Text();
//    	private Text query_word = new Text();
    	private String query_word;
    
    	
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            // Implementation of you mapper function
        	String sentence = value.toString().replaceAll("[^A-Za-z0-9]", " ").toLowerCase();
        	String[] words = sentence.trim().split("\\s+");
        	HashSet<String> seen = new HashSet<String>();
        	HashMap<String, Integer> map = new HashMap<String, Integer>();
        	
        	for (int i = 0; i < words.length; i++) {
        		if(!seen.add(words[i])) {
        			continue;
        		}

        		context_word.set(words[i]);
        		for (int j = 0; j < words.length; j++) {
        			if (i != j) {
//        				query_word.set(words[j]);
        				query_word = words[j];
        				
        				if (map.containsKey(query_word)) {
        					map.put(query_word, new Integer(map.get(query_word) + 1));

        				}
        				else {
        					map.put(query_word, new Integer(1));
        				}
        				
//        				context.write(context_word, new SimpleEntry(query_word, one));
        			}
        		}
        		
            	for (String s: map.keySet()) {
            		context.write(context_word, new QueryPair(s, map.get(s)));
            	}
            	
            	map.clear();
        	}
  
        }
    }

    // Replace "?" with your own key / value types
    // NOTE: combiner's output key / value types have to be the same as those of mapper
    public static class TextCombiner extends Reducer<Text, QueryPair, Text, QueryPair> {
        public void reduce(Text key, Iterable<QueryPair> tuples, Context context)
            throws IOException, InterruptedException
        {
            // Implementation of you combiner function
        	HashMap<String, Integer> map = new HashMap<String, Integer>();
        	String query_word;
        	for (QueryPair entry: tuples) {
        		query_word = entry.getKey();
        		if (map.containsKey(query_word)) {
    				map.put(query_word, map.get(query_word) + entry.getValue());
    			}
    			else {
    				map.put(query_word, entry.getValue());
    			}
        	}
        	for (String s: map.keySet()) {
        		context.write(key, new QueryPair(s, map.get(s)));
        	}
			
        }
    }

    // Replace "?" with your own input key / value types, i.e., the output
    // key / value types of your mapper function
    public static class TextReducer extends Reducer<Text, QueryPair, Text, Text> {
        private final static Text emptyText = new Text("");
    	private Text queryWordText = new Text();
        public void reduce(Text key, Iterable<QueryPair> queryTuples, Context context)
            throws IOException, InterruptedException
        {
            // Implementation of you reducer function
//        	System.out.println("REDUCER INPUT: " + queryTuples);
        	TreeMap<String, Integer> map = new TreeMap<String, Integer>(); 
        	int max = 0;
        	for (QueryPair entry: queryTuples) {
        		int count = entry.getValue();
        		
        		if (count > max) {
        			max = count; 
        		}
        		
        		map.put(entry.getKey(), count);
        	}
        	
            // Write out the results; you may change the following example
            // code to fit with your reducer function.
            //   Write out the current context key
            context.write(key, new Text(String.valueOf(max)));
            
        	for (String queryWord: map.keySet()) {
        		int num = map.get(queryWord).intValue();
        		if (num == max) {
        			String count = String.valueOf(num) + ">";
                    queryWordText.set("<" + queryWord + ",");
                    context.write(queryWordText, new Text(count));
                    
        		}
        	}

            //   Write out query words and their count
            for(String queryWord: map.keySet()){
            	int num = map.get(queryWord).intValue();
            	if (num != max) {
            		String count = num + ">";
                    queryWordText.set("<" + queryWord + ",");
                    context.write(queryWordText, new Text(count));
            	}
            }
            //   Empty line for ending the current context key
            context.write(emptyText, emptyText);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "jlh6554_kh33248"); // Replace with your EIDs
        job.setJarByClass(TextAnalyzer.class);

        // Setup MapReduce job
        job.setMapperClass(TextMapper.class);
        //   Uncomment the following line if you want to use Combiner class
        job.setCombinerClass(TextCombiner.class);
        job.setReducerClass(TextReducer.class);

        // Specify key / value types (Don't change them for the purpose of this assignment)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //   If your mapper and combiner's  output types are different from Text.class,
        //   then uncomment the following lines to specify the data types.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(QueryPair.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // Do not modify the main method
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TextAnalyzer(), args);
        System.exit(res);
    }

    // You may define sub-classes here. Example:
     public static class QueryPair implements Writable {
    	Text word;
    	IntWritable count;
    	
    	public QueryPair() {
    		word = new Text("EMPTY");
    		count = new IntWritable(-1);
    	}
    	
    	public QueryPair(String s, Integer c) {
    		word = new Text(s);
    		count = new IntWritable(c.intValue());
    	}
		
		@Override
		public void readFields(DataInput in) throws IOException {
	        word.readFields(in);
	        count.readFields(in);
	    }
		@Override
	    public void write(DataOutput out) throws IOException {
	        word.write(out);
	        count.write(out);
	    }
	    
	    public String getKey() {
	    	return word.toString();
	    }
	    
	    public int getValue() {
	    	return count.get();
	    }
	    
	    @Override 
	    public String toString() {
	    	return "<" + word.toString() + ", " + count.toString() + ">";
	    }
     }
    

}



