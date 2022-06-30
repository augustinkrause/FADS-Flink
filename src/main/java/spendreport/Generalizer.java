/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 *
 */
public class Generalizer<T extends Tuple> extends ProcessFunction<Tuple2<T, Long>, T> {

	int k;
	long bufferConstraint;
	long reuseConstraint;

	int[] keys;
	int pidKey;

	Tuple2<Double, Double>[] globalBounds;

	PriorityQueue<Tuple2<T, Long>> bufferedTuples;
	PriorityQueue<Tuple2<Cluster<T>, Long>> bufferedClusters;

	Stack<T> toBeReleasedTuples = new Stack<>(); //during processing of the stream tuples will be added to this, at the end of processing the stack will be emptied

	public Generalizer(int k, long bufferConstraint, long reuseConstraint, int[] keys, int pidKey){
		this.k = k;
		this.bufferConstraint = bufferConstraint; //how long can a tuple stay at max in the buffer, before being released
		this.reuseConstraint = reuseConstraint; //how long can a cluster be reused
		this.globalBounds = new Tuple2[keys.length];

		this.keys = keys; //these are the indices of the fields to be considered QIDs
		this.pidKey = pidKey;

		bufferedTuples = new PriorityQueue<>(new EnrichedTupleComparator()); //so that our tuples are always sorted by our used notion of time
		this.bufferedClusters = new PriorityQueue<>(new EnrichedTupleComparator());
	}
	// This hook is executed before the processing starts, kind of as a set-up
	@Override
	public void open(Configuration parameters){

	}

	// This hook is executed on each element of the data stream
	@Override
	public void processElement(
			Tuple2<T, Long> element,
			Context context,
			Collector<T> collector) throws Exception {

		//buffer incoming tuple
		bufferedTuples.add(element);

		//update global bounds (TODO: For now we only support one key, at index 0)
		if (this.globalBounds[0] == null) {
			this.globalBounds[0] = new Tuple2<>(element.f0.getField(this.keys[0]), element.f0.getField(this.keys[0]));
		}else{
			if(this.globalBounds[0].f0 > (double) element.f0.getField(this.keys[0])) this.globalBounds[0].f0 = element.f0.getField(this.keys[0]);
			if(this.globalBounds[0].f1 < (double) element.f0.getField(this.keys[0])) this.globalBounds[0].f1 = element.f0.getField(this.keys[0]);
		}

		//remove expired cluster(s)
		while(!this.bufferedClusters.isEmpty()){
			if(this.bufferedClusters.peek().f1 + this.reuseConstraint < System.currentTimeMillis()) this.bufferedClusters.poll();
		}

		//generalize oldest tuple if buffer constraint is exceeded
		if(this.bufferedTuples.size() > this.bufferConstraint){
			this.generalizeTuple(this.bufferedTuples.poll());
		}

		//release all tuples that have been generalized in the last round
		while(!this.toBeReleasedTuples.isEmpty()){
			collector.collect(this.toBeReleasedTuples.pop());
		}
	}

	//either releases a tuple along with the other tuples from its k-anonymized cluster, or the tuple gets suppressed and then released
	private void generalizeTuple(Tuple2<T, Long> t){
		if(this.bufferedTuples.size() < this.k - 1){
			//in this case we can't form a new k-anonymized cluster around the tuple
			//so either we reuse an old cluster or suppress the tuple
			Cluster<T> minC = this.findFittingOldCluster(t.f0);
			if(minC != null){
				//release tuple with an old cluster, that has minimal information loss
				T newT = minC.generalize(t.f0);
				this.toBeReleasedTuples.add(newT);
			}else{
				//suppress tuple
				T newT = this.suppress(t.f0);
				this.toBeReleasedTuples.add(newT);
			}
		}else{
			//find k-1 NNs with unique PIDs of t
			Tuple2<T, Long>[] neighbours = this.knn(t.f0);
			Cluster<T> knnC = null;
			if(neighbours != null){
				knnC = new Cluster<T>(neighbours, this.keys);
			}

			//find old cluster with minimal information loss
			Cluster<T> minC = this.findFittingOldCluster(t.f0);

			if(knnC != null){
				if(minC != null && minC.infoLoss(this.globalBounds) < knnC.infoLoss(this.globalBounds)){
					//release tuple with old cluster, that has minimal information loss
					T newT = minC.generalize(t.f0);
					this.toBeReleasedTuples.add(newT);
				}else{
					//release tuple with its k-1 NNs
					for(int i = 0; i < neighbours.length; i++){
						T newT = knnC.generalize(neighbours[i].f0);
						this.bufferedTuples.remove(neighbours[i]); //remove the current tuple from the buffered tuples
						this.toBeReleasedTuples.add(newT);
					}
					T newT = knnC.generalize(t.f0);
					this.toBeReleasedTuples.add(newT);
				}
			}else{
				if(minC != null){
					//release tuple with an old cluster, that has minimal information loss
					T newT = minC.generalize(t.f0);
					this.toBeReleasedTuples.add(newT);
				}else{
					//suppress tuple
					T newT = this.suppress(t.f0);
					this.toBeReleasedTuples.add(newT);
				}
			}

		}
	}


	//finds the k-1 NNs with unique PIDs of t
	private Tuple2<T, Long>[] knn(T t){
		Tuple3<Tuple2<T, Long>, T, Tuple2<Double, Double>[]>[] toBeSorted = new Tuple3[this.bufferedTuples.size()];

		//retrieve and sort all the tuples by their distance to t
		Iterator<Tuple2<T, Long>> it = this.bufferedTuples.iterator();
		int index = 0;
		while(it.hasNext()){
			toBeSorted[index] = new Tuple3(it.next(), t, this.globalBounds);
			index++;
		}

		Arrays.sort(toBeSorted, new DistanceComparator<T>()); //sorts the tuples other than t by their "distance" to t (Distance as defined in the FADS-paper)

		//only take the k-1 with unique PID of the sorted ones
		Tuple2<T, Long>[] neighbours = new Tuple2[this.k - 1];
		int count = 0;
		HashMap<Double, Boolean> uniqueKeys = new HashMap<>();
		for(int i = 0; i < toBeSorted.length && count < this.k - 1; i++){
			//only add the next tuple if it has a unique pid
			if(!uniqueKeys.containsKey(toBeSorted[i].f0.f0.getField(this.pidKey))){
				neighbours[count] = toBeSorted[i].f0;
				uniqueKeys.put(toBeSorted[i].f0.f0.getField(this.pidKey), true);
				count++;
			}
		}

		if(count == this.k - 1){
			//we have found enough neighbours and can return them
			return neighbours;
		}else{
			//otherwise the calling function knows we haven't found enough neighbours by returning null
			return null;
		}

	}

	//finds amongst all buffered clusters the cluster that fits t and has minimal information loss
	private Cluster findFittingOldCluster(Tuple t){
		Iterator<Tuple2<Cluster<T>, Long>> it = this.bufferedClusters.iterator();
		Cluster minC = null;
		while(it.hasNext()){
			Cluster c = it.next().f0;
			if(minC == null){
				if(c.fits(t)) minC = c;
			}else{
				if(c.fits(t) && c.infoLoss(this.globalBounds) < minC.infoLoss(this.globalBounds)) minC = c;
			}
		}

		return minC;
	}

	//returns a tuple, the entries of which correspond to the global bounds encountered during the whole processing duration
	private T suppress(T t){
		T newT = (T) Tuple.newInstance(t.getArity());
		newT.setField(this.globalBounds[0], 0);

		return newT;
	}

	static class EnrichedTupleComparator implements Comparator<Tuple2<?, Long>> {

		public int compare(Tuple2<?, Long> o1, Tuple2<?, Long> o2) {
			return o1.f1 > o2.f1 ? 1 : -1;
		}
	}

	//TODO: This is sooooooo uglyyy :(
	static class DistanceComparator<T extends Tuple> implements Comparator<Tuple3<Tuple2<T, Long>, T, Tuple2<Double, Double>[]>> {

		public int compare(Tuple3<Tuple2<T, Long>, T, Tuple2<Double, Double>[]> o1, Tuple3<Tuple2<T, Long>, T, Tuple2<Double, Double>[]> o2) {
			return (Math.abs((double)o1.f0.f0.getField(0) - (double)o1.f1.getField(0)) / o1.f2[0].f1 - o1.f2[0].f0) >
					(Math.abs((double)o2.f0.f0.getField(0) - (double)o2.f1.getField(0)) / o2.f2[0].f1 - o2.f2[0].f0) ? 1 : -1;
		}
	}

}
