import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.lang.StringBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.fs.FileSystem;

public class PRAdjust {
    
    public static class PRAdjustMapper
            extends Mapper<Object, Text, IntWritable, PRNodeWritable>{
	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		PRNodeWritable node = new PRNodeWritable(value.toString());
		IntWritable one = new IntWritable(1);
		context.write(one,node);
	}
    }
    public static class PRAdjustReducer
            extends Reducer<IntWritable,PRNodeWritable,NullWritable,PRNodeWritable> {

	    private Double alpha;
	    private Integer number_node;
	    private Double mass = 0.0;
	    private List<PRNodeWritable> list;
            protected void setup(Context context)  throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
                this.alpha = Double.parseDouble(conf.get("alpha"));
		this.number_node = Integer.parseInt(conf.get("number_node"));
		list = new ArrayList<PRNodeWritable>();
            }

	public void reduce(IntWritable key, Iterable<PRNodeWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
		for(PRNodeWritable val : values){
			mass+=val.rank;
			PRNodeWritable tmp = new PRNodeWritable();
			tmp.copy(val);
			list.add(tmp);
		}
	}
	 public void cleanup(Context context) throws IOException, InterruptedException {
		mass = 1.0 - mass;
                for(int i=0;i<this.list.size();i++){
			//context.write(NullWritable.get(),list.get(i));
			PRNodeWritable tmp_node = new PRNodeWritable();
			tmp_node.copy(list.get(i));
			Double tmp_rank = tmp_node.rank;
			tmp_rank = alpha*(1.0/number_node) + (1.0-alpha)*(tmp_rank + mass/number_node);
			tmp_node.rank = tmp_rank;
			context.write(NullWritable.get(),tmp_node);
		}
            }
	
    }
   
}
