package fads;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.util.regex.Pattern;

public class CSVParser implements MapFunction<String, Tuple>, ResultTypeQueryable {

    int nCols;
    TypeInformation[] types;
    String delimiter = ",";
    boolean addPID = false;
    int linesPerSecond = -1;
    int count = 0;
    long startTime;

    public CSVParser(Integer nCols, TypeInformation[] types, String delimiter, boolean addPID, int linesPerSecond) throws Exception{
        if(types.length != nCols){
            throw new RuntimeException("You need to specify as many types as there are columns!");
        }
        this.nCols = nCols;
        this.types = types;
        this.delimiter = delimiter;
        this.addPID = addPID;
        this.linesPerSecond = linesPerSecond;
        this.startTime = System.currentTimeMillis();
    }

    public CSVParser(Integer nCols, TypeInformation[] types, String delimiter) throws Exception{
        if(types.length != nCols){
            throw new RuntimeException("You need to specify as many types as there are columns!");
        }
        this.nCols = nCols;
        this.types = types;
        this.delimiter = delimiter;
        this.startTime = System.currentTimeMillis();
    }

    public CSVParser(Integer nCols, TypeInformation[] types){
        if(types.length != nCols){
            throw new RuntimeException("You need to specify as many types as there are columns!");
        }
        this.nCols = nCols;
        this.types = types;
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public Tuple map(String line) throws Exception {

        String[] tokens = line.split(Pattern.quote(this.delimiter));
        if (tokens.length != this.nCols) {
            throw new RuntimeException("Invalid record: " + line);
        }

        try{
            Tuple t = this.addPID? Tuple.newInstance(this.nCols + 1) : Tuple.newInstance(this.nCols);
            if(this.addPID){
                t.setField(count, 0);
            }
            for(int i = 0; i < this.nCols; i++){
                if(this.types[i].getTypeClass().isInstance((Integer) 1)){
                    t.setField(Integer.parseInt(tokens[i]), this.addPID? i + 1 : i);
                }else if(this.types[i].getTypeClass().isInstance((Double) 1.)){
                    t.setField(Double.parseDouble(tokens[i]), this.addPID? i + 1 : i);
                }else{
                    t.setField(tokens[i], this.addPID? i + 1 : i);
                }
            }

            this.count++;
            if(this.linesPerSecond > 0 && count % this.linesPerSecond == 0){
                while(System.currentTimeMillis() < this.startTime + 1000){
                    //active waiting
                }
                this.startTime = System.currentTimeMillis();
            }
            return t;
        }catch(NumberFormatException nfe) {
            throw new RuntimeException("Invalid field: " + line, nfe);
        }
    }

    @Override
    public TypeInformation getProducedType() {
        if(this.addPID){
            TypeInformation[] returnTypes = new TypeInformation[this.nCols + 1];
            returnTypes[0] = Types.INT;
            for(int i = 0; i < this.nCols; i++){
                returnTypes[i + 1] = this.types[i];
            }
            return Types.TUPLE(returnTypes);
        }
        return Types.TUPLE(this.types);
    }
}
