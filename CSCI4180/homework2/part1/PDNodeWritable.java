import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
 
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;

public class PDNodeWritable implements Writable {
       public int isNode;
       public int nodeType;//0 is unreachable, 1 is new found node, 2 is old found node     
       public int nodeID;
       public int distance;
       public Map<Integer,Integer> adjMap;
       
       public void write(DataOutput out) throws IOException {
	 out.writeInt(this.isNode);
	 out.writeInt(this.nodeType);
	 out.writeInt(this.nodeID);
	 out.writeInt(this.distance);
	 out.writeInt(this.adjMap.size());

	 Iterator<Map.Entry<Integer,Integer>> it = this.adjMap.entrySet().iterator();
	 while(it.hasNext()){
		Map.Entry<Integer,Integer> entry = it.next();
		out.writeInt(entry.getKey());
		out.writeInt(entry.getValue());
	 }
       }
       
       public void readFields(DataInput in) throws IOException {
	 this.isNode = in.readInt();
	 this.nodeType = in.readInt();
         this.nodeID = in.readInt();
	 this.distance = in.readInt();
	 int number = in.readInt();
	 this.adjMap = new HashMap<Integer,Integer>();
	 for(int i=0;i<2*number;i+=2){
		Integer Key = in.readInt();
		Integer Value = in.readInt();
		this.adjMap.put(Key,Value); 
	 }
	 
       }

	public PDNodeWritable(){

	}

	public static PDNodeWritable read(DataInput in) throws IOException {
         PDNodeWritable w = new PDNodeWritable();
         w.readFields(in);
         return w;
       }
       
       public PDNodeWritable(int nodeID, int distance, int isNode){
	 this.nodeID = nodeID;
	 this.distance = distance;
	 this.isNode = isNode;
	 this.adjMap = new HashMap<Integer,Integer>();
	 this.nodeType=0;
       }

       public PDNodeWritable(String s){
	 this.adjMap = new HashMap<Integer,Integer>();
	 String parts[] = s.split("\\s+");
	 this.isNode = Integer.parseInt(parts[0]);
	 this.nodeType = Integer.parseInt(parts[1]);
	 this.nodeID = Integer.parseInt(parts[2]);
	 this.distance = Integer.parseInt(parts[3]);
	 int number = Integer.parseInt(parts[4]);
	 for(int i=5;i<(5+2*number);i+=2){
		Integer Key = Integer.parseInt(parts[i]);
		Integer Value = Integer.parseInt(parts[i+1]);
		this.adjMap.put(Key,Value);
	 }
	 
       }

       public void addNeighbour(int nodeID, int distance){
	 this.adjMap.put(nodeID,distance);	
       }

       public void copy(PDNodeWritable new_node){
	 this.isNode = new_node.isNode;
	 this.nodeType = new_node.nodeType;
	 this.nodeID = new_node.nodeID;
	 this.distance = new_node.distance;
	 this.adjMap = new HashMap<Integer,Integer>();
	 Iterator<Map.Entry<Integer,Integer>> it = new_node.adjMap.entrySet().iterator();
	 while(it.hasNext()){
                Map.Entry<Integer,Integer> entry = it.next();
                this.adjMap.put( entry.getKey() , entry.getValue() );
         }
       }

       public String toString(){
	String s ="";
	s+= Integer.toString(isNode) +" " + Integer.toString(nodeType) +" " + Integer.toString(nodeID) +" " + Integer.toString(distance) +" " + Integer.toString(adjMap.size()) +" ";
	Iterator<Map.Entry<Integer,Integer>> it = this.adjMap.entrySet().iterator();
         while(it.hasNext()){
                Map.Entry<Integer,Integer> entry = it.next();
                s += entry.getKey().toString() + " " + entry.getValue().toString() + " ";
         }
	return s;
       }
     }
