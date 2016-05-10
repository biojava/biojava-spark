package org.biojava.spark.function;

/**
 * Created by ap3 on 29/04/2016.
 */

import org.apache.spark.api.java.function.Function;

import org.biojava.nbio.alignment.Alignments;
import org.biojava.nbio.alignment.SimpleGapPenalty;
import org.biojava.nbio.alignment.template.GapPenalty;
import org.biojava.nbio.alignment.template.PairwiseSequenceAligner;
import org.biojava.nbio.core.alignment.matrices.SubstitutionMatrixHelper;
import org.biojava.nbio.core.alignment.template.AlignedSequence;
import org.biojava.nbio.core.alignment.template.SequencePair;
import org.biojava.nbio.core.alignment.template.SubstitutionMatrix;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.core.sequence.compound.AminoAcidCompound;
import scala.Tuple2;

import scala.Tuple5;




/**
 * Performs a pairwise alignment and returns a Tuple5.
 * These are the returned elements:
 * - name1
 * - name2
 * - overlap1
 * - overlap2
 * - percentage identical residues in the alignment
 */
public class PairwiseSequenceComparison implements Function<Tuple2<Tuple2<String,String>,Tuple2<String,String> >, Tuple5<String,String,Float,Float,Float>> {

    private static final boolean debug = false;

    @Override
    public Tuple5<String,String,Float,Float,Float> call(Tuple2<Tuple2<String,String>,Tuple2<String,String>> tuple) throws Exception {

        Tuple2<String,String> p1 = tuple._1();
        Tuple2<String,String> p2 = tuple._2();

        SubstitutionMatrix matrix = SubstitutionMatrixHelper.getBlosum65();
        GapPenalty penalty = new SimpleGapPenalty();
        penalty.setOpenPenalty(8);
        penalty.setExtensionPenalty(1);

        ProteinSequence prot1 = new ProteinSequence(p1._2());
        ProteinSequence prot2 = new ProteinSequence(p2._2());

        try {
            PairwiseSequenceAligner<ProteinSequence, AminoAcidCompound> smithWaterman =
                    Alignments.getPairwiseAligner(
                            prot1,
                            prot2,
                            Alignments.PairwiseSequenceAlignerType.LOCAL,
                            penalty,
                            matrix);

            SequencePair<ProteinSequence, AminoAcidCompound> alignment = smithWaterman.getPair();

            if ( debug )
             System.out.println(alignment.toString(60));



            int numIdenticals = alignment.getNumIdenticals();

            int aligLength = alignment.getLength();

            float percentIdenticals = (numIdenticals / (float) aligLength);


            // test overlaps

            int l1 = prot1.getLength();
            int l2 = prot2.getLength();

            int size = alignment.getLength();

            AlignedSequence alignedSequence1 = alignment.getAlignedSequence(1);
            AlignedSequence alignedSequence2 = alignment.getAlignedSequence(2);

            String alignedSeq1 = alignedSequence1.getSequenceAsString().replaceAll("-","");
            String alignedSeq2 = alignedSequence2.getSequenceAsString().replaceAll("-","");

            int size1 = alignedSeq1.length();
            int size2 = alignedSeq2.length();

            float overlap1 = size1 / (float) l1;
            float overlap2 = size2 / (float) l2;

            if ( debug )
                System.out.println(p1._1() + " " + p2._1() + " size:" + size + " l1: " + l1 + " l2: " + l2 + " overlap1 " + overlap1 + " overlap2 " + overlap2 + " %id: " + percentIdenticals);

            return new Tuple5<String, String, Float,Float,Float>(p1._1(),p2._1(),overlap1,overlap2,percentIdenticals);
        } catch (Exception e) {
            System.err.println("Could not align " + p1._1() + " vs." + p2._1());
        }


        return new  Tuple5<String, String, Float,Float,Float>(p1._1(),p2._1(),0f,0f,0f);
    }


}
