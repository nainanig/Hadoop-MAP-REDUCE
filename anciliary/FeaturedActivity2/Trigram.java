import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.String;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




//import java.util.TreeSet;


public class Trigram{
	public static class WordCooccurences extends Mapper<Object, Text, Text, IntWritable>
	{
private final static Text KEY= new Text();
private final static IntWritable ONE=new IntWritable(1);

public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
String[] line=value.toString().replaceAll("//s+"," ").trim().split("\n");
for(String l:line){
	String[] words=l.trim().split(" ");
	for(String word:words){
		//if(word.equals(" ")) continue;
		for (String ne:words) {
      for(String n1:words){

			//if(n.equals(" ")) continue;
			if(!word.equals("")&&!ne.equals("")&&!n1.equals("")&&!word.equals(ne)&&!word.equals(n1)){

				KEY.set(word+" "+ne+" "+n1);
				//System.out.println(KEY);
				context.write(KEY,ONE);}
			}
			
		}
	}
}}}




	
	public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word pair");
    job.setJarByClass(Trigram.class);
    job.setMapperClass(WordCooccurences.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
