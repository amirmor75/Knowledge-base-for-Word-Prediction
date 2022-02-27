package jobs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job6CalcProb {



    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private long C0;
        @Override
        protected void setup(Context context)
        {
            C0 = context.getConfiguration().getLong("C0", -1);
        }
        public double calcProb(double N1,double N2,double N3,double C0, double C1, double C2 ) {
            double k2 = (java.lang.Math.log(N2 + 1) + 1) / (java.lang.Math.log(N2 + 1) + 2);
            double k3 = (java.lang.Math.log(N3 + 1) + 1) / (java.lang.Math.log(N3 + 1) + 2);
            return (k3 * (N3 / C2) + (1 - k3) * k2 * (N2 / C1) + (1 - k3) * (1 - k2) * (N1 / C0));
        }


        /**
         * @param key     ⟨w<sub>1</sub>⟩
         * @param values   ⟨optional(w<sub>1</sub>,w<sub>2</sub>),sum⟩
         * @param context we write the ( [w<sub>1</sub> , w<sub>2</sub>] , [w1<sub>count</sub>,w1w2<sub>count</sub>]) pair
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text outKey ;
            Text outVal;
            int N1=-1,N2=-1,N3=-1,C1=-1,C2=-1 ;
            String[] keyWords = key.toString().split(" ");
            String triWord1 = keyWords[0],
                    triWord2 = keyWords[1];
            for (Text value : values) {
                String[] valSplit = value.toString().split(" ");
                String word1 = valSplit[0];

                int w1Count=Integer.parseInt(valSplit[2]),
                        pairCount=Integer.parseInt(valSplit[3]),
                        trigramCount= Integer.parseInt(valSplit[4])
                ;
                if(word1.equals(triWord1)){
                    C2 = pairCount;
                    C1 = w1Count;
                    N3 = trigramCount;
                }
                if(word1.equals(triWord2)){
                    N1 = w1Count;
                    N2 = pairCount;
                }
            }
            double prob = calcProb(N1,N2,N3,C0,C1,C2);
            outKey = new Text(String.format("%s %s %s %f",keyWords[0],keyWords[1],keyWords[2],prob));
            outVal = new Text("-");
            context.write(outKey,outVal);
        }

    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

}





