import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import java.util.*;
import java.io.*;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Lemma {
  public static HashMap<String,String> test=new HashMap<String,String>();
public static class TextMapper
       extends Mapper<Object, Text, Text, Text>{
        private final static Text KEY= new Text();
        private final static Text START=new Text();
        public void map(Object key, Text value, Context context)
  throws IOException, InterruptedException{
    String read_file1=value.toString().toLowerCase();
    String[]ar=read_file1.split("\\t");
    START.set(ar[0]);
    //System.out.print(START);
   //System.out.println(" "+ar[1]);
   String[]tokens=ar[1].split("\\s+");
     for(int i=0;i<tokens.length;i++)
    {
    	 String change1=tokens[i].toString().replaceAll("j","i");
      String change2=change1.replaceAll("v","u");
     
      if(test.containsKey(change2)){
        //System.out.println("key"+change2);
        String str=test.get(change2);
        String[]a=str.split(":");
        for(int j=0;j<a.length;j++){
         //System.out.println("lemma "+a[j]);
         Text ts=new Text(a[j]);
          context.write(ts,START);
        }}
         context.write(new Text(change2),START);
     }
 }
}
public static class TextMapperTwo
       extends Mapper<Object, Text, Text, Text>{
        private final static Text KEY= new Text();
        private final static Text START=new Text();
        public void map(Object key, Text value, Context context)
  throws IOException, InterruptedException{
    String read_file1=value.toString().toLowerCase();
    String[]ar=read_file1.split("\\t");
    START.set(ar[0]);
    //System.out.print(START);
   //System.out.println(" "+ar[1]);
   String[]tokens=ar[1].split("\\s+");
     for(int i=0;i<tokens.length;i++)
    {
    	 String change1=tokens[i].toString().replaceAll("j","i");
      String change2=change1.replaceAll("v","u");
     
      if(test.containsKey(change2)){
        //System.out.println("key"+change2);
        String str=test.get(change2);
        String[]a=str.split(":");
        for(int j=0;j<a.length;j++){
         //System.out.println("lemma "+a[j]);
         Text ts=new Text(a[j]);
          context.write(ts,START);
        }}
         context.write(new Text(change2),START);
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
   	Path firstPath = new Path(args[0]);
	Path sencondPath = new Path(args[1]);
	//Path outputPath = new Path(args[2]);
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "lemma");
    job.setJarByClass(Lemma.class);
     job.setMapperClass(TextMapper.class);
     job.setMapperClass(TextMapperTwo.class);
     job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    MultipleInputs.addInputPath(job, firstPath, TextInputFormat.class, TextMapper.class);
	MultipleInputs.addInputPath(job, sencondPath, TextInputFormat.class, TextMapperTwo.class);

	//FileOutputFormat.setOutputPath(job, outputPath);
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
   // FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}