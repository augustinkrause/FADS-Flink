package spendreport;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.StringUtils;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

public class Cluster<T> {

    public Cluster(){
    }

    static class ElementComparator implements Comparator<Tuple2<?, Long>> {

        @Override
        public int compare(Tuple2<?, Long> o1, Tuple2<?, Long> o2) {
            return o1._2.longValue() > o2._2.longValue() ? 1 : -1;
        }
    }
}
