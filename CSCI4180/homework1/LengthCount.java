import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LengthCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, IntWritable>{

            private final static IntWritable lengthCount = new IntWritable();
            private Text word = new Text();
	    private IntWritable wordLength = new IntWritable();
	    private Map<Integer,Integer> map = new HashMap<Integer,Integer>();
            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
		    Integer mapkey = new Integer(word.getLength());
		    if(map.containsKey(mapkey)){
			Integer total = new Integer(map.get(mapkey).intValue()+1);
			map.put(mapkey,total);
		    }else{
			map.put(mapkey,new Integer(1));
		    }
                }
                    }
	    protected void cleanup(Context context)
  		throws IOException, InterruptedException {
 		 
 		 Iterator<Map.Entry<Integer, Integer>> it = map.entrySet().iterator();
 		 while(it.hasNext()) {
 		  Map.Entry<Integer, Integer> entry = it.next();
 		  Integer iKey = entry.getKey();
 		  int total = entry.getValue().intValue();
 		  context.write(new IntWritable(iKey.intValue()), new IntWritable(total));
 		 }
 		}
    }

    public static class IntSumReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(IntWritable key, Iterable<IntWritable> values,
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
        Job job = Job.getInstance(conf, "length count");
        job.setJarByClass(LengthCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
