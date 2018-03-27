import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.HashMap;
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
    public static class TextMapper extends Mapper<LongWritable, Text, Text, SimpleEntry> {
    	private final static IntWritable one = new IntWritable(1);
    	private Text context_word = new Text();
//    	private Text query_word = new Text();
    	private String query_word;
    
    	
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            // Implementation of you mapper function
        	String sentence = value.toString().toLowerCase().replaceAll("[^A-Za-z0-9]", " ");
        	String[] words = sentence.split(" ");
        	HashSet<String> seen = new HashSet<String>();
        	HashMap<String, SimpleEntry<String, Integer>> map = new HashMap<String, SimpleEntry<String, Integer>>();
        	
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
        					SimpleEntry<String, Integer> temp = map.get(query_word);
        					temp.setValue(temp.getValue() + 1);
        					map.put(query_word, temp);

        				}
        				else {
        					map.put(query_word, new SimpleEntry(query_word, 1));
        				}
        				
//        				context.write(context_word, new SimpleEntry(query_word, one));
        			}
        		}
        		
            	for (String s: map.keySet()) {
            		context.write(context_word, map.get(s));
            	}
            	
            	map.clear();
        	}
  
        }
    }

    // Replace "?" with your own key / value types
    // NOTE: combiner's output key / value types have to be the same as those of mapper
    public static class TextCombiner extends Reducer<Text, SimpleEntry, Text, SimpleEntry> {
        public void reduce(Text key, Iterable<SimpleEntry> tuples, Context context)
            throws IOException, InterruptedException
        {
            // Implementation of you combiner function
        }
    }

    // Replace "?" with your own input key / value types, i.e., the output
    // key / value types of your mapper function
    public static class TextReducer extends Reducer<Text, SimpleEntry, Text, Text> {
        private final static Text emptyText = new Text("");

        public void reduce(Text key, Iterable<SimpleEntry> queryTuples, Context context)
            throws IOException, InterruptedException
        {
            // Implementation of you reducer function

            // Write out the results; you may change the following example
            // code to fit with your reducer function.
            //   Write out the current context key
            context.write(key, emptyText);
            //   Write out query words and their count
//            for(String queryWord: map.keySet()){
//                String count = map.get(queryWord).toString() + ">";
//                queryWordText.set("<" + queryWord + ",");
//                context.write(queryWordText, new Text(count));
//            }
            //   Empty line for ending the current context key
            context.write(emptyText, emptyText);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "EID1_EID2"); // Replace with your EIDs
        job.setJarByClass(TextAnalyzer.class);

        // Setup MapReduce job
        job.setMapperClass(TextMapper.class);
        //   Uncomment the following line if you want to use Combiner class
        // job.setCombinerClass(TextCombiner.class);
        job.setReducerClass(TextReducer.class);

        // Specify key / value types (Don't change them for the purpose of this assignment)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //   If your mapper and combiner's  output types are different from Text.class,
        //   then uncomment the following lines to specify the data types.
        //job.setMapOutputKeyClass(?.class);
        //job.setMapOutputValueClass(?.class);

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
//     public static class pair implements Tuple {
//    

}



