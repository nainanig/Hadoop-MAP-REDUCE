import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Stripe_Words{
public static class SMapper extends Mapper<LongWritable,Text,Text,CoOccMap>
{
	private CoOccMap map=new CoOccMap();
	private Text KEY=new Text();
	public void map(LongWritable key,Text value, Context context)throws IOException,InterruptedException{
		String[]words=value.toString().replaceAll("\\s+"," ").trim().split(" ");
		int len=words.length;
		if(len>1){
			for(String t:words){
				KEY.set(t);
				map.clear();
			}
		
		for(String word:words){
              Text neighbor=new Text(word);
              if(map.containsKey(neighbor)){
              	IntWritable c=(IntWritable)map.get(neighbor);
              	c.set(c.get()+1);
              }else{
              	map.put(neighbor,new IntWritable(1));
              }
		}
		context.write(KEY,map);

	}
}



}
public static class SReducer extends Reducer<Text,MapWritable,Text,CoOccMap>{
private CoOccMap map=new CoOccMap();
public void reduce (Text key,Iterable<MapWritable> values,Context context)throws IOException,InterruptedException{
	map.clear();
	for(MapWritable v:values){
Set<Writable> k = v.keySet();
	for (Writable keys:k) {
		IntWritable fromCount = (IntWritable) v.get(keys);
            if (map.containsKey(keys)) {
                IntWritable count = (IntWritable) map.get(keys);
                count.set(count.get() + fromCount.get());
            } else {
                map.put(keys, fromCount);
            }
	}

}
context.write(key, map);


}}
public static class CoOccMap extends MapWritable {
   
    public String toString() {
        StringBuilder result = new StringBuilder();
        Set<Writable> keySet = this.keySet();

        for (Object key : keySet) {
            result.append("{" + key.toString() + " = " + this.get(key) + "}");
        }
        return result.toString();
    }
}
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word stripes");
    job.setJarByClass(Stripe_Words.class);
    job.setMapperClass(SMapper.class);
    job.setCombinerClass(SReducer.class);
    job.setReducerClass(SReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CoOccMap.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);




  }
}
//References : http://codingjunkie.net/cooccurrence/
//References : http://stackoverflow.com/questions/23209174/converting-mapwritable-to-a-string-in-hadoop