import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.lang.StringBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ngramRelaFreq {
    

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

            private final static IntWritable one = new IntWritable(1);
            
	    private ArrayList<String> list = new ArrayList<String>();
	    private Map<String,Integer> map = new HashMap<String,Integer>();
	    private int N;
	    private int index=0;
	    protected void setup(Context context)  throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
                N = Integer.parseInt(conf.get("N"));
	    }
            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		
		StringBuffer delims = new StringBuffer("");
		for(int i=0;i<65;i++)
		    delims.append(String.valueOf((char)i));
		for(int i=91;i<97;i++)
                    delims.append(String.valueOf((char)i));
		for(int i=123;i<128;i++)
                    delims.append(String.valueOf((char)i));
                StringTokenizer itr = new StringTokenizer(value.toString(),delims.toString());
		
		
                while(itr.hasMoreTokens()) {
		    String s = itr.nextToken();
                    
		    if(index<(N-1)){
			list.add(s);
			index++;
		    }else{
			list.add(s);
			Text skey = new Text();
			String final_s = "";
			for(String t : list){
			    final_s+=t.substring(0,1)+" ";
			    
			}
			skey.set(final_s);
			context.write(skey,one);
			//put into map
			String mapkey = final_s.substring(0,1);
			if(map.containsKey(mapkey)){
			    Integer total = new Integer(map.get(mapkey).intValue()+1);
			    map.put(mapkey,total);
			}else{
			    map.put(mapkey,new Integer(1));
			}

			list.remove(0);
		    }
		    
                }
		
            }
	    public void cleanup(Context context) throws IOException, InterruptedException {
		Iterator<Map.Entry<String,Integer>> it = map.entrySet().iterator();
		while(it.hasNext()){
		    Map.Entry<String,Integer> entry = it.next();
		    Text skey = new Text(entry.getKey()+" *");
		    IntWritable total = new IntWritable(entry.getValue().intValue());
		    context.write(skey,total);
		}
	    }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,DoubleWritable> {
            private DoubleWritable result = new DoubleWritable();
	    private int total = 0;
	    private char current_char = 0;
	    private double s =0.0;
	    protected void setup(Context context)  throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
                s = Double.parseDouble(conf.get("s"));
            }
            public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
		//[a *] must come first comparing to [a b c], since * is int 42
		if(key.toString().charAt(0) != current_char){
		    current_char = key.toString().charAt(0);
		    total = sum;
		}else{
		    if((double)sum/total >= s){
			result.set((double)sum/total);
			context.write(key, result);
		    }
		}
                
                    }
    }

    public static class SumPartitioner extends Partitioner<Text, IntWritable> {
		
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			//same first char must go to same reducer
			int first_char = Character.getNumericValue(key.charAt(0));
			return first_char % numReduceTasks;
		}
	}

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
	conf.set("N",args[2]);
	conf.set("s",args[3]);
	conf.set("mapreduce.textoutputformat.separator", " ");
        Job job = Job.getInstance(conf, "N-gram Relative Frequency Count");
	job.setNumReduceTasks(2);
        job.setJarByClass(ngramRelaFreq.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
	job.setPartitionerClass(SumPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
