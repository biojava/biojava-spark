package org.biojava.spark.pair;

/**
 * Created by ap3 on 29/04/2016.
 */

import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.alignment.Alignments;
import org.biojava.nbio.alignment.SimpleGapPenalty;
import org.biojava.nbio.alignment.template.GapPenalty;
import org.biojava.nbio.alignment.template.PairwiseSequenceAligner;
import org.biojava.nbio.core.alignment.matrices.SubstitutionMatrixHelper;
import org.biojava.nbio.core.alignment.template.SequencePair;
import org.biojava.nbio.core.alignment.template.SubstitutionMatrix;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.core.sequence.compound.AminoAcidCompound;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;


/**
 * Created by ap3 on 27/04/2016.
 */
public class PairwiseSequenceComparison implements PairFunction<Tuple2<Integer,Integer>,Tuple2<String,String>,Boolean>, Serializable {


    List<Tuple2<String,String>> sequences;

    float minOverlap;
    float minPercid;

    public PairwiseSequenceComparison( List<Tuple2<String,String>> sequences, float minOverlap, float minPercid){
        this.sequences = sequences;
        this.minOverlap = minOverlap;
        this.minPercid = minPercid;
    }

    @Override
    public Tuple2<Tuple2<String,String>,Boolean> call(Tuple2<Integer,Integer> tuple) throws Exception {

        int i1 = tuple._1();
        int i2 = tuple._2();

        Tuple2<String, String> p1 = sequences.get(i1);
        Tuple2<String, String> p2 = sequences.get(i2);

        if (p1 == null || p2 == null || p1._2() == null || p2._2() == null) {

            String n1 = "";
            if (p1 != null && p1._1() != null)
                n1 = p1._1();

            String n2 = "";
            if (p2 != null && p2._1() != null)
                n2 = p2._1();

            Tuple2<String, String> names = new Tuple2<String, String>(n1, n2);

            return new Tuple2<Tuple2<String, String>, Boolean>(names, false);

        }

        SubstitutionMatrix matrix = SubstitutionMatrixHelper.getBlosum65();
        GapPenalty penalty = new SimpleGapPenalty();
        penalty.setOpenPenalty(8);
        penalty.setExtensionPenalty(1);

        ProteinSequence prot1 = new ProteinSequence(p1._2());
        ProteinSequence prot2 = new ProteinSequence(p2._2());

        try {
            PairwiseSequenceAligner<ProteinSequence, AminoAcidCompound> smithWaterman = Alignments.getPairwiseAligner(prot1,
                    prot2,
                    Alignments.PairwiseSequenceAlignerType.LOCAL, penalty, matrix);

            SequencePair<ProteinSequence, AminoAcidCompound> alignment = smithWaterman.getPair();

            // System.out.println(alignment.toString(60));


            // test percentage ID in the alignment


            int numIdenticals = alignment.getNumIdenticals();

            int aligLength = alignment.getLength();

            float percentIdenticals = (numIdenticals / (float) aligLength);


            // test overlaps

            int l1 = prot1.getLength();
            int l2 = prot2.getLength();

            int size = alignment.getLength();

            float overlap1 = l1 / (float) size;
            float overlap2 = l2 / (float) size;

            //System.out.println(p1._1() + " " + p2._1() + " size:" + size + " l1: " + l1 + " l2: " + l2 + " overlap1 " + overlap1 + " overlap2 " + overlap2 + " %id: " + percentIdenticals);


            boolean isSimilar = true;

            if (minPercid > percentIdenticals)
                isSimilar = false;

            if (minOverlap > overlap1 || minOverlap > overlap2)
                isSimilar = false;


            Tuple2<String, String> names = new Tuple2<String, String>(p1._1(), p2._1());

            return new Tuple2<Tuple2<String, String>, Boolean>(names, isSimilar);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Tuple2<String, String> names = new Tuple2<String, String>(p1._1(), p2._1());

        return new Tuple2<Tuple2<String, String>, Boolean>(names, false);
    }

}
