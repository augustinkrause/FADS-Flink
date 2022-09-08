package spendreport;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.io.Serializable;

public class Cluster implements Serializable{

    private Tuple2<Double, Double>[] bounds;
    private int[] keys;


    public Cluster(Tuple[] tuples, int[] keys){

        this.keys = keys;
        this.bounds = new Tuple2[keys.length];

        //find global bounds per dimension to enclose all the tuples
        for(int i = 0; i < keys.length; i++){
            double currMin = Double.POSITIVE_INFINITY;
            double currMax = Double.NEGATIVE_INFINITY;
            for(int j = 0; j < tuples.length; j++){
                if(currMin > ((Number) tuples[j].getField(this.keys[i])).doubleValue()) currMin = ((Number) tuples[j].getField(this.keys[i])).doubleValue(); //update minimum
                if(currMax < ((Number) tuples[j].getField(this.keys[i])).doubleValue()) currMax = ((Number) tuples[j].getField(this.keys[i])).doubleValue(); //update maximum
            }

            this.bounds[i] = new Tuple2<>(currMin, currMax);
        }
    }

    public Cluster(Tuple2<Tuple, Long>[] tuples, int[] keys){

        this.keys = keys;
        this.bounds = new Tuple2[keys.length];

        //find global bounds per dimension to enclose all the tuples
        for(int i = 0; i < keys.length; i++){
            double currMin = Double.POSITIVE_INFINITY;
            double currMax = Double.NEGATIVE_INFINITY;
            for(int j = 0; j < tuples.length; j++){
                if(currMin > ((Number)  tuples[j].f0.getField(this.keys[i])).doubleValue()) currMin = ((Number) tuples[j].f0.getField(this.keys[i])).doubleValue(); //update minimum
                if(currMax < ((Number) tuples[j].f0.getField(this.keys[i])).doubleValue()) currMax = ((Number) tuples[j].f0.getField(this.keys[i])).doubleValue(); //update maximum
            }

            this.bounds[i] = new Tuple2<>(currMin, currMax);
        }
    }

    //checks whether t lies within the bounds of each dimension
    public boolean fits(Tuple t){

        boolean check = true;
        for(int i = 0; i < this.keys.length; i++){
            check = check && (((Number) t.getField(this.keys[i])).doubleValue() > this.bounds[i].f0 && ((Number) t.getField(this.keys[i])).doubleValue() < this.bounds[i].f1);
        }
        return check;
    }

    //returns a tuple, the entries of which correspond to the bounds of this cluster
    public Tuple generalize(Tuple t){
        Tuple newT = Tuple.newInstance(t.getArity());

        for(int i = 0; i < this.keys.length; i++){
            newT.setField(this.bounds[i], this.keys[i]);
        }

        for(int i = 0; i < t.getArity(); i++){
            if(newT.getField(i) == null) newT.setField(t.getField(i), i);
        }

        return newT;
    }

    public double infoLoss(Tuple2<Double, Double>[] globalBounds){
        double infoLoss = 0;
        for(int i = 0; i < this.keys.length; i++){
            infoLoss += (this.bounds[i].f1 - this.bounds[i].f0) / (globalBounds[i].f1 - globalBounds[i].f0);
        }
        return infoLoss/this.keys.length;
    }

    //readObject, writeObject, readObjectNoData are functions that are called during serialization of instances of this class
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {

        out.writeInt(this.bounds.length);
        for(int i = 0; i < this.bounds.length; i++){
            out.writeObject(this.bounds[i]);
        }

    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {

        int len = in.readInt();
        this.bounds = new Tuple2[len];
        for(int i = 0; i < len; i++){
            this.bounds[i] = (Tuple2<Double, Double>) in.readObject();
        }

    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
    }

    @Override
    public String toString() {
        return "Cluster{" +
                "bounds=" + StringUtils.arrayAwareToString(bounds) +
                '}';
    }
}
