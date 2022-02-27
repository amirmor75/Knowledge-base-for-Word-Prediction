package jobs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;


public class Job5Zip3With4 {

    public static class MapperClass extends Mapper<Text, Text, Text, Text> {


        /**
         * @param key     [w<sub>1</sub>,w<sub>2</sub>,w<sub>3</sub>] /or/ [w<sub>1</sub>,w<sub>2</sub>]
         * @param value   [sum<sub>w1w2w3</sub>] /or/ [sum<sub>w1</sub>,sum<sub>w1w2</sub>]
         * @param context write ([w<sub>1</sub> , w<sub>2</sub>],[w<sub>1</sub>,w<sub>2</sub>,w<sub>3</sub>,sum<sub>w1w2w3</sub>]),
         *              ([w<sub>2</sub> , w<sub>3</sub>],[w<sub>1</sub>,w<sub>2</sub>,w<sub>3</sub>,sum<sub>w1w2w3</sub>])
         *                   <br>/or/ ([w<sub>1</sub>,w<sub>2</sub>],[sum<sub>w1</sub>,sum<sub>w1w2</sub>])
         */
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] keySplit = key.toString().split(" ");
            Text outKey = new Text();
            Text outval = new Text();
            if( keySplit.length == 3 ){ // the key is a Trigram so we send also inverted tri
                outKey.set(String.format("%s %s %s",keySplit[1],keySplit[2],keySplit[0]));
                outval.set(String.format("%s %s","w2w3w1",value.toString()));
                context.write(outKey, outval);
            }
            context.write(key, value);

        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private int pairCount = -1;
        private int singleWordCount = -1;
        private final Text outKey = new Text();
        private final Text outVal = new Text();
        /**
         * @param key     [w<sub>1/2</sub>,w<sub>2/3</sub>]
         * @param values   [w<sub>1</sub>,w<sub>2</sub>,w<sub>3</sub>,sum<sub>w1w2w3</sub>] or [sum<sub>w1</sub>,sum<sub>w1w2</sub>] or [sum<sub>w2</sub>,sum<sub>w2w3</sub>]
         * @param context write ( [w<sub>1</sub> , w<sub>2</sub>, w<sub>3</sub>] , [w<sub>1</sub>,w<sub>2</sub>,sum<sub>w1</sub>,sum<sub>w1w2</sub>, sum<sub>w1w2w3</sub>])
         *  <br>or ( [w<sub>1</sub> , w<sub>2</sub>, w<sub>3</sub>] , [w<sub>2</sub>,w<sub>3</sub>,sum<sub>w2</sub>,sum<sub>w2w3</sub>, sum<sub>w1w2w3</sub>])
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] keyWords = key.toString().split(" ");

            if(keyWords.length == 2 ){
                String value = values.iterator().next().toString();
                String[] valSplit = value.split(" ");
                singleWordCount= Integer.parseInt(valSplit[0]);
                pairCount= Integer.parseInt(valSplit[1]);
            }
            else{
                if(singleWordCount!= -1 & keyWords.length == 3 ){
                    for (Text value : values) {
                        String[] valSplit = value.toString().split(" ");
                        String[] keySplit = key.toString().split(" ");
                        if(valSplit.length == 1){ // regular tri
                            outKey.set(key);
                            outVal.set(String.format("%s %s %d %d %d",
                                    keySplit[0],keySplit[1],singleWordCount,pairCount,Integer.parseInt(value.toString())));
                            context.write(outKey, outVal);
                        }
                        if(valSplit.length == 2){ // inverted tri
                            outKey.set(String.format("%s %s %s",keySplit[2],keySplit[0],keySplit[1])); // invert back
                            outVal.set(String.format("%s %s %d %d %d",
                                    keySplit[0],keySplit[1],singleWordCount,pairCount,Integer.parseInt(value.toString())));
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
            int i2 = str.indexOf(" ", str.indexOf(" ") + 1);
            return (( i2==-1 ? str : str.substring(0, i2)).hashCode() & Integer.MAX_VALUE) % numPartitions;
//            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

}




