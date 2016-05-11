package demo;

import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.biojava.spark.function.TmScorer;
import org.rcsb.mmtf.spark.data.Segment;

import scala.Tuple2;
import scala.Tuple3;

/**
 * Mapper function to perform TMScore analysis.
 * @author Anthony Bradley
 *
 */
public class TmScore implements Function<Tuple2<Integer,Integer>,Tuple3<Long, Long, Double>> {

	/**
	 * The serial version of this commit.
	 */
	private static final long serialVersionUID = -2557285958500330806L;
	private Broadcast<List<Tuple2<String, Segment>>> calphaChains;
	
	/**
	 * Constructor - populates with the list for the comparisons.
	 * @param calphaChains2 the input chains
	 */
	public TmScore(Broadcast<List<Tuple2<String, Segment>>> calphaChains2) {
		this.calphaChains = calphaChains2;
	}
	
	@Override
	public Tuple3<Long, Long, Double> call(Tuple2<Integer, Integer> t) throws Exception {
		Segment segmentOne = calphaChains.getValue().get(t._1)._2;
		Segment segmentTwo = calphaChains.getValue().get(t._2)._2;
		double score = TmScorer.getFatCatTmScore(segmentOne, segmentTwo);
		return new Tuple3<Long, Long, Double>(Integer.toUnsignedLong(t._1),Integer.toUnsignedLong(t._2), 1.0-score);
	}



}
