package jobs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;



public class Job4Zip1With2 {
    public static class Mapper1Gram extends Mapper<Text, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outVal = new Text();

        /**
         * @param key     w or [w<sub>1</sub>,w<sub>2</sub>]
         * @param value   sum of word or pair
         * @param context we write [⟨w<sub>1</sub>⟩,⟨optional(w<sub>1</sub>,w<sub>2</sub>),sum⟩]
         */
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
//            String[] words = key.toString().split(" ");

//            String occurences = String.valueOf(Integer.parseInt(value.toString()));
//            if(words.length == 1) { // the key is one word
//                outKey.set(String.format("%s", words[0]));
//                outVal.set(occurences);
//            }
//            else if(words.length == 2){ // the key is a 2 word pair
//                outKey.set(String.format("%s %s",words[1]));
//                outVal.set(String.format("%d",words[0],words[1],Integer.parseInt(value.toString())));
//            }
//             else {
//                System.out.println("job4 mapper got bad word as input: "+ key);
//                key.set(key+"אאאאא");
//                context.write(key , value);
//            }
            context.write(key , value);


        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();
        private int singleWordCount = -1;

        /**
         * @param key     ⟨w<sub>1</sub>⟩
         * @param values   ⟨optional(w<sub>1</sub>,w<sub>2</sub>),sum⟩
         * @param context we write the ( [w<sub>1</sub> , w<sub>2</sub>] , [w1<sub>count</sub>,w1w2<sub>count</sub>]) pair
         */

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] keyWords = key.toString().split(" ");
            if (keyWords.length==1)
            {
                singleWordCount= Integer.parseInt(values.iterator().next().toString());
            }
            else{
                if (singleWordCount != -1 & keyWords.length==2 ) {
                    for (Text value : values) {
                        String[] valSplit = value.toString().split(" ");
                        if (valSplit.length == 1) {
                            outKey.set(key);
                            outVal.set(String.format("%d %d", singleWordCount, Integer.parseInt(valSplit[0])));
                            context.write(outKey, outVal);
                        }
                    }
                }
            }
        }
    }


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String str = key.toString();
            int i = str.indexOf(" ");
            return (( i==-1 ? str : str.substring(0, str.indexOf(" "))).hashCode() & Integer.MAX_VALUE) % numPartitions;
            //return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

}



