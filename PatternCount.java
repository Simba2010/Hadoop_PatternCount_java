/*
 * This Code is Open Source
 * Do whatever you want with it!
 * HAKUNA MATATA - Don't worry be happy :)
 * Check stackoverflow.com for more Happy code!
 * Code contains COMMENTS throughout to help you understand
 */
 
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.FileSystem;
         
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

//
import java.io.InputStreamReader;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import java.io.BufferedReader;
import java.io.FileReader;

public class PatternCount {
	/* Mapper get the tweets per line so there is no 
	 * need to split the line by words since we are not counting the words
	 * initialize every 
	 */
	public static class Map extends MapReduceBase 
		implements Mapper<LongWritable, Text, Text, LongWritable > {

		private final static LongWritable one = new LongWritable(1);
		private Text tweet = new Text();

		public void map(LongWritable key, Text value, 
		OutputCollector<Text, LongWritable> output, 
		Reporter reporter) throws IOException {
			String line = value.toString(); //assuming one line is a tweet
			tweet.set(line);
			output.collect(tweet, one);
		}
	}

	/* Reducer just get tweets and tally pertaining frequency
	 * simple is fun
	 */
	public static class Reduce extends MapReduceBase 
	implements Reducer<Text,LongWritable , Text, LongWritable> {
		public void reduce(Text key, Iterator<LongWritable> values,
		OutputCollector<Text, LongWritable> output, 
		Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key,new LongWritable(sum));
		}
	}
	
	/* Ok, now that we have the tweets and their occurences
	 * let's load the regular expressions into a hashset
	 * the mapper just collects the tweet and frequency
	 * if the regular expression matches (most time consuming job)
	 * note the regular expression file is on hadoop
	 */
	public static class Pattern extends MapReduceBase
		implements Mapper<LongWritable, Text, Text, LongWritable > {
		private Text tweet = new Text();
		private Set<String> patternList = new HashSet<String>();
		//private Configuration conf;
		private BufferedReader fis;
		
		public Pattern() throws IOException, InterruptedException {
			String patternFileName = "hdfs://localhost:PORT/location/of/file/patternList.txt";
			
			getPatternsFromFile(patternFileName);
		}
		
		private void getPatternsFromFile(String fileName) {
			try {
				Path pt=new Path(fileName);
				FileSystem fs = FileSystem.get(new Configuration());
				
				fis = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String pattern = null;
				
				while ((pattern = fis.readLine()) != null) {
				  patternList.add(pattern);
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '"
					+ StringUtils.stringifyException(ioe));
			}
		}

		public void map(LongWritable key, Text value,
		OutputCollector<Text, LongWritable> output, 
		Reporter reporter) throws IOException {
			String line = value.toString();
			String record[] = line.split("\\t");
			int frequency = Integer.parseInt(record[1]);
			
			tweet.set(record[0]);
			String tempTweet = tweet.toString();
			for (String pattern : patternList) {
				if( tempTweet.matches(pattern) ) {
					output.collect( new Text(pattern), new LongWritable(frequency));
				}
			}
		}
	}
	
	/* Just sum every frequency of the regular expression matches
	 * perhaps unnecessary at this point to reverse the order or the ouput
	 * anyway it's done and there is no repercussion on the outcome
	 * because of this the order is reversed again in the next mapper :(
	 */
	public static class PatternReduce extends MapReduceBase 
	implements Reducer<Text,LongWritable , LongWritable, Text> {
		public void reduce(Text key, Iterator<LongWritable> values,
		OutputCollector<LongWritable, Text> output, 
		Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(new LongWritable(sum),key);
		}
	}
    
	
	 /* Using the parent class to compare and sort reverse and using opposite
	 * value of compare output
	 */
	public static class Sort extends WritableComparator {
		public Sort() {
			super(LongWritable.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			LongWritable a1 = (LongWritable)a;
			LongWritable b1 = (LongWritable)b;
			return a1.compareTo(b1); // super -super.compare(a, b); also works :)
		}
		
		public int compare(byte[] a1, int b1, int c1, byte[] a2, int b2, int c2) {
			return -super.compare(a1, b1, c1, a2, b2, c2);
		}
	}
	

	public static class SortMap extends MapReduceBase 
		implements Mapper<LongWritable, Text, LongWritable, Text > {
		public void map(LongWritable key, Text value, 
		OutputCollector<LongWritable, Text> output, 
		Reporter reporter) throws IOException {
			String line = value.toString();
			String record[] = line.split("\\t");
			int val = Integer.parseInt(record[0]);
			output.collect(new LongWritable(val), new Text(record[1]));
		}
	}
	
	/* This reduce just reverses the output so that you can get it in the format
	 * we want Tweet->Frequency and not Frequency->Tweet
	 * quite simple to just collect the values
	 */
	public static class SortReduce extends MapReduceBase 
	implements Reducer<LongWritable,Text , Text, LongWritable> {
		public void reduce(LongWritable key, Iterator<Text> values,
		OutputCollector<Text, LongWritable> output, 
		Reporter reporter) throws IOException {
			Text val = new Text();
			while (values.hasNext()) {
				val = values.next();
				output.collect(val, key);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Path tempFile = new Path("wcs-temp-" + Integer.toString( new Random().nextInt(Integer.MAX_VALUE) ));
		Path tempSortedFile = new Path("sorted-temp-" + Integer.toString( new Random().nextInt(Integer.MAX_VALUE) ));
		int reduceTasks = 1;
		int mapTasks = 5;
		
		WritableComparator.define( PatternCount.class , new Sort());

		System.out.println("1. New JobConf...");
		JobConf conf = new JobConf(PatternCount.class);
		conf.setJobName("PatternCount");
        
		System.out.println("2. Setting output key and value..."); 
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
          
		System.out.println("3. Setting Mapper and Reducer classes..."); 
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		// set numbers of reducers
		System.out.println("4. Setting number of reduce and map tasks...");
		conf.setNumReduceTasks(reduceTasks);
		conf.setNumMapTasks(mapTasks);
          
		System.out.println("5. Setting input and output formats..."); 
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
        //
		System.out.println("6. Setting input and output paths..."); 
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, tempFile);//
        
		System.out.println("7. Running job..."); 
		JobClient.runJob(conf);
		//end count config
		
		// Config to match patterns against tweets
		 
		JobConf patternConf = new JobConf(PatternCount.class);
		patternConf.setJobName("pattern");
		patternConf.setMapperClass(Pattern.class);
		patternConf.setReducerClass(PatternReduce.class);
		//
		patternConf.setOutputKeyClass(Text.class);
		patternConf.setOutputValueClass(LongWritable.class);
		
		patternConf.setNumReduceTasks(reduceTasks);
		patternConf.setNumMapTasks(mapTasks);
		
		patternConf.setInputFormat( TextInputFormat.class );
		patternConf.setOutputFormat( TextOutputFormat.class );
		
		FileInputFormat.setInputPaths(patternConf, tempFile);
		FileOutputFormat.setOutputPath(patternConf, tempSortedFile);
		JobClient.runJob(patternConf);
		//
		
		/* Finally sorting ...
		 * the mapper output is reversed so that we can sort by value
		 * basically out value(frequency) becomes the key
		 * the reducers reverses the order
		 * back to key->value
		 */
		JobConf sortConf = new JobConf(PatternCount.class);
		sortConf.setJobName("PatternCount_2");
		//
		sortConf.setMapperClass(SortMap.class);
		sortConf.setReducerClass(SortReduce.class);
		//
		sortConf.setOutputKeyClass(LongWritable.class);
		sortConf.setOutputValueClass(Text.class);
		//
		sortConf.setNumReduceTasks(reduceTasks);
		//
		sortConf.setInputFormat(TextInputFormat.class);
		sortConf.setOutputFormat(TextOutputFormat.class);
		//
		FileInputFormat.setInputPaths(sortConf, tempSortedFile);
		FileOutputFormat.setOutputPath(sortConf, new Path(args[1]));
		
		// this magic line to reverse
		sortConf.setOutputKeyComparatorClass(Sort.class);
		JobClient.runJob(sortConf);
	}
}
