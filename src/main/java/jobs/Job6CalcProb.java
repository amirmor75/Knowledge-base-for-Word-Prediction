package jobs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import writables.Pair3Numbers;
import writables.Trigram;
import writables.TrigramWithProb;

import java.io.IOException;

public class Job6CalcProb {



    public static class ReducerClass extends Reducer<Trigram, Pair3Numbers, TrigramWithProb, Text> {
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


        @Override
        public void reduce(Trigram key, Iterable<Pair3Numbers> values, Context context) throws IOException, InterruptedException {
            TrigramWithProb outKey;
            Text outVal ;
            int N1=-1,N2=-1,N3=-1,C1=-1,C2=-1 ;

            for (Pair3Numbers value : values) {
                if(value.getWord1().toString().equals(key.getWord1().toString())){
                    C2 = value.getPairCount().get();
                    C1 = value.getW1Count().get();
                    N3 = value.getTrigramCount().get();
                }
                if(value.getWord1().toString().equals(key.getWord2().toString())){
                    N1 = value.getW1Count().get();
                    N2 = value.getPairCount().get();
                }
            }
            double prob = calcProb(N1,N2,N3,C0,C1,C2);
            outKey = new TrigramWithProb(key.getWord1().toString(),key.getWord2().toString(),key.getWord3().toString(),prob);
            outVal = new Text("-");
            context.write(outKey,outVal);
        }

    }

    public static class PartitionerClass extends Partitioner<Trigram, Pair3Numbers> {
        @Override
        public int getPartition(Trigram key, Pair3Numbers value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

}





