package org.biojava.spark.function;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Random;

/** Maps a key to a randomly generated key.
 *
 * Created by ap3 on 05/05/2016.
 */
public class RandomKeyAssigner implements Function<String, Tuple2<Integer,String>> {

    int nrFractions = 10;

    static Random random = new Random();

    public RandomKeyAssigner(int nrFramctions) {
        this.nrFractions = nrFramctions;
    }

    @Override
    public Tuple2<Integer, String> call(String s) throws Exception {

        Tuple2<Integer,String> tuple = new Tuple2<>(random.nextInt(nrFractions),s);

        return tuple;

    }
}
