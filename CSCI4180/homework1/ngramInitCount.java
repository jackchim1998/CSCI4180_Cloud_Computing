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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ngramInitCount {
    

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();
	    private ArrayList<String> list = new ArrayList<String>();
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
                    word.set(s);
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
			list.remove(0);
		    }
		    
                }
		
                    }
    }

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
	conf.set("N",args[2]);
	conf.set("mapreduce.textoutputformat.separator", " ");
        Job job = Job.getInstance(conf, "N-gram Initial Count");
        job.setJarByClass(ngramInitCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
