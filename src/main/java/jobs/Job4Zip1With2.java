package jobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class Job4Zip1With2 {
    // change LongWritable to text
    public static class Mapper1Gram extends Mapper<Text, IntWritable, Text, Text> {

        private final Text outKey = new Text();
        private final Text outval = new Text();
        private boolean debug =true;
        /**
         * @param key     ⟨w⟩
         * @param value   ⟨sum⟩
         * @param context ⟨⟩
         */
        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String[] words = key.toString().split(" ");
            if (debug)
            {
                System.out.println("my first key: "+ key);
                debug=false;
            }

            if(words.length == 2){ // the key is a 2 word pair
                outKey.set(String.format("%s",words[1]));
                outval.set(String.format("%s %s %d",words[0],words[1],value.get()));
            }
            else if(words.length == 1){ // the key is one word
                outKey.set(String.format("%s",words[0]));
                outval.set(String.valueOf(value.get()));
            } else {
                System.out.println("job4 mapper got bad word as input: "+ key);
                return;
            }
            context.write(outKey , outval);
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();
        /**
         * @param key     ⟨w<sub>1</sub>,optional(w<sub>2</sub>)⟩
         * @param values   ⟨sum⟩
         * @param context ⟨⟩
         */

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int singleWordCount = -1 ;

            for (Text value : values) {
                String[] words = value.toString().split(" ");
                if(words.length==1){
                    singleWordCount= Integer.parseInt(words[0]);
                }
            }
            for (Text value : values) {
                String[] words = value.toString().split(" ");
                if(words.length>1){
                    outKey.set(String.format("%s %s",words[0],words[1]));
                    outVal.set(String.format("%d %d",singleWordCount,Integer.parseInt(words[2])));
                    context.write(outKey,outVal);
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

}



