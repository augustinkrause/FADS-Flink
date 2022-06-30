package spendreport;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

public class Cluster{

    Tuple2<Double, Double>[] bounds;

    public Cluster(Tuple[] tuples, int[] keys){
        this.bounds = new Tuple2[keys.length];

        //find global bounds per dimension to enclose all the tuples
        double currMin = Double.POSITIVE_INFINITY;
        double currMax = Double.NEGATIVE_INFINITY;
        for(int i = 0; i < tuples.length; i++){
            if(currMin > (double) tuples[i].getField(0)) currMin = tuples[i].getField(0); //update minimum
            if(currMax < (double) tuples[i].getField(0)) currMax = tuples[i].getField(0); //update maximum
        }

        this.bounds[0] = new Tuple2<>(currMin, currMax);
    }

    public Cluster(Tuple2<Tuple, Long>[] tuples, int[] keys){
        this.bounds = new Tuple2[keys.length];

        //find global bounds per dimension to enclose all the tuples
        double currMin = Double.POSITIVE_INFINITY;
        double currMax = Double.NEGATIVE_INFINITY;
        for(int i = 0; i < tuples.length; i++){
            if(currMin > (double) tuples[i].f0.getField(0)) currMin = tuples[i].f0.getField(0); //update minimum
            if(currMax < (double) tuples[i].f0.getField(0)) currMax = tuples[i].f0.getField(0); //update maximum
        }

        this.bounds[0] = new Tuple2<>(currMin, currMax);
    }

    //checks wether t lies within the bounds of each dimension
    public boolean fits(Tuple t){

        return (double) t.getField(0) > this.bounds[0].f0 && (double) t.getField(0) < this.bounds[0].f1;
    }

    //returns a tuple, the entries of which correspond to the bounds of this cluster
    public Tuple generalize(Tuple t){
        Tuple newT = Tuple.newInstance(t.getArity());
        newT.setField(this.bounds[0], 0);

        return newT;
    }

    public double infoLoss(Tuple2<Double, Double>[] globalBounds){
        return (this.bounds[0].f1 - this.bounds[0].f0) / (globalBounds[0].f1 - globalBounds[0].f0);
    }

}
