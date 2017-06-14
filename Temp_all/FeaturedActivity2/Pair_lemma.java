import java.io.IOException;
import java.util.*;
import java.lang.String;
import java.io.*;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.lang.*;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Pair_lemma{
   public static HashMap<String,String> test=new HashMap<String,String>();
	public static class WordCooccurences extends Mapper<Object, Text, Text, Text>
	{
private final static Text KEY= new Text();
//private final static IntWritable ONE=new IntWritable(1);

public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
  ArrayList<String> temp1=new ArrayList<String>();
  ArrayList<String> temp2=new ArrayList<String>();
  String l1="",l2="";
  String in=value.toString().toLowerCase();
String[] line=in.replaceAll("\\s+"," ").trim().split("\n");
for(String l:line){
  String[]l_break=l.split(">");
  String loc=l_break[0]+">";
	String lo=l_break[1].replaceAll("\\p{P}","");
  String[]words=lo.trim().split(" ");
	for(String word:words){
		//if(word.equals(" ")) continue;
     String change1=word.toString().replaceAll("j","i");
      String change2=change1.replaceAll("v","u");
    if(test.containsKey(change2)){
      temp1.add(test.get(change2));
    }
		for (String ne:words) {
       String change3=ne.toString().replaceAll("j","i");
      String change4=change3.replaceAll("v","u");
			//if(n.equals(" ")) continue;
      if(test.containsKey(change4)){
       temp2.add(test.get(change4));
      }
			if(!word.equals("")&&!ne.equals("")&&!word.equals(ne)){
       

				KEY.set(word+" "+ne);
				System.out.println(KEY);
				context.write(KEY,new Text(loc));
        if(temp1.size()!=0 && temp2.size()!=0){
          l1=temp1.get(0);
          l2=temp2.get(0);
          String[]a1=l1.split(":");
          String[]a2=l2.split(":");
          int i1=a1.length;
          int i2=a2.length;
          int index1, index2;
          if(i1>i2){  index1=i1;
             index2=i2;

          for(int i=0;i<index1;i++){
            for(int j=0;j<index2;j++){
              KEY.set(a1[i]+" "+a2[j]);
              context.write(KEY,new Text(loc));
            }

        }}
            else{
               index1=i2;
               index2=i1;

          for(int i=0;i<index1;i++){
            for(int j=0;j<index2;j++){
              KEY.set(a2[i]+" "+a1[j]);
              System.out.println("lemma "+KEY);
              context.write(KEY,new Text(loc));
            }

        }
            }

          }

			}
			
		}
	}
}
}
}


public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
     String print_out="";
      for (Text val : values) {
        print_out=print_out+":"+val.toString();
      }
      result.set(print_out);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "pair lemma");
    job.setJarByClass(Pair_lemma.class);
    job.setMapperClass(WordCooccurences.class);
     //job.setMapperClass(WordCooccurences2.class);
      job.setMapOutputKeyClass(Text.class);
  job.setMapOutputValueClass(Text.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
     //MultipleInputs.addInputPath(job, firstPath, TextInputFormat.class, WordCooccurences.class);
  //MultipleInputs.addInputPath(job, sencondPath, TextInputFormat.class, WordCooccurences2.class);
    
   
    BufferedReader b=null;
    try{
     b=new BufferedReader(new FileReader("/home/hadoop/Downloads/new_lemmatizer.csv"));
     String l;
     while((l=b.readLine())!=null){
      String[] tempArray = l.split("\\s*,");
      int len=tempArray.length;
      String key=tempArray[0]; String val="";
      for(int i=1;i<len;i++){
         val+=tempArray[i]+":";
      }
     test.put(key,val);

     }

    }catch(FileNotFoundException f){
      System.out.println(f);
    }
     //FileOutputFormat.setOutputPath(job, outputPath);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
