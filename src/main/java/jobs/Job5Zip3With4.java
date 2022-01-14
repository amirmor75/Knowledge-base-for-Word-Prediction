package jobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import writables.Pair3Numbers;
import writables.Trigram;

import java.io.IOException;


public class Job5Zip3With4 {

    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outval = new Text();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = key.toString().split(" ");
            String[] values = value.toString().split(" ");
            if( words.length == 3 ){ // the key is a Trigram
                outKey.set(String.format("%s %s",words[0],words[1]));
                outval.set(String.format("%s %s %s %d",words[0],words[1], words[2],Integer.parseInt(values[1])));
                context.write(outKey, outval);
                outKey.set(String.format("%s %s",words[1],words[2]));
                context.write(outKey, outval);
            }
            else{ // the key is a pair of words
                outKey.set(String.format("%s %s",words[1],words[2]));
                outval.set(String.format("%d %d", Integer.parseInt(values[1]),Integer.parseInt(values[2])));
                context.write(outKey, outval);
            }

        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Trigram, Pair3Numbers> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Trigram outKey;
            Pair3Numbers outVal ;
            int pairCount = -1;
            int singleWordCount = -1;
            for (Text value : values) {
                String[] valSplit = value.toString().split(" ");
                if(valSplit.length==2){
                    singleWordCount= Integer.parseInt(valSplit[0]);
                    pairCount= Integer.parseInt(valSplit[1]);
                }
            }
            for (Text value : values) {
                String[] valSplit = value.toString().split(" ");
                String[] keySplit = key.toString().split(" ");
                if(valSplit.length==4){
                    outKey = new Trigram(valSplit[0],valSplit[1],valSplit[2]);
                    outVal = new Pair3Numbers( String.format("%s", keySplit[0]),String.format("%s",keySplit[1]),singleWordCount,pairCount,Integer.parseInt(valSplit[3]));
                    context.write(outKey, outVal);
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

}




