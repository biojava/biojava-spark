package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureException;
import org.biojava.nbio.structure.align.util.AtomCache;
import org.biojava.nbio.structure.ecod.EcodFactory;
import org.biojava.nbio.structure.ecod.EcodInstallation;
import org.biojava.spark.ClusterSequences;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by ap3 on 29/04/2016.
 */
public class ClusterEcodSequences implements Serializable {


    public static String CSV_FILE = "/Users/ap3/WORK/GIT/pathway_spark/src/main/resources/mapping_recon3_ECOD.csv";




    private static final int NR_THREADS = Runtime.getRuntime().availableProcessors();
    private static final int TASKS_PER_THREAD = 3;
    private static final int BATCH_SIZE=100;

    private static final float MIN_OVERLAP = .9f;
    private static final float MIN_PERCID  = .4f;


    public static void main(String[] args) {

        String pdbDir = args[0];
        System.setProperty("PDB_DIR",pdbDir);

        System.out.println(pdbDir);

        ClusterEcodSequences me = new ClusterEcodSequences();

        me.clusterEcodSequences();



    }


//    static EcodInstallation ecod ;

    public void clusterEcodSequences(){

        SparkConf conf = new SparkConf()
                .setMaster("local[" + NR_THREADS+"]")
                .setAppName(this.getClass().getSimpleName())
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer");


        // read the PDB to ECOD mapping file

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy");

        sqlContext.setConf("spark.sql.parquet.filterPushdown", "true");


        DataFrame df = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema","true").option("header","true").load(CSV_FILE);

        df.printSchema();

        //df = df.limit(50);



        df.show();


        DataFrame ecodDomains = df.select("ecod_domain_id");


        // just to be sure, remove duplicates.
        ecodDomains = ecodDomains.distinct();

        ecodDomains.show();

        System.out.println("Retrieving " + ecodDomains.count() +" ECOD domain sequences...");
        JavaPairRDD<String,String> sequences =  ecodDomains.javaRDD().mapToPair(new PairFunction<Row, String, String>(){
            @Override
            public Tuple2<String,String> call(Row row){
                String ecodId = row.getString(0);
                try {
                    return new Tuple2(ecodId, getSequence(ecodId));
                } catch (Exception e){
                    e.printStackTrace();
                    return null;
                }
            }

        });

        sequences.cache();

        long timeS = System.currentTimeMillis();

        ClusterSequences cluster = new ClusterSequences(10, MIN_OVERLAP, MIN_OVERLAP, MIN_PERCID);

        JavaPairRDD masterRDD = cluster.clusterSequences(sc,sequences,5);

        masterRDD = masterRDD.repartition(1);
        masterRDD.saveAsTextFile( "ecod_pathway_seqclust.csv");

        long timeE = System.currentTimeMillis();

        long runTime = timeE-timeS;
        System.out.println("clustering took " + (runTime)/1000 + " sec. ");

        long n = ecodDomains.count();
        long combinations = n * (n-1) / 2;

        System.out.println("Average time per comparison: " + (runTime / combinations) + " ms.");


        sc.close();
        System.exit(0);
    }




    public  String getSequence(String ecodId) throws IOException, StructureException {


        AtomCache cache = new AtomCache();

       // cache.setUseMmtf(true);

        try {
            Structure s = cache.getStructure(ecodId);
            StringBuffer seq = new StringBuffer();

            for ( Chain c : s.getChains()) {

                seq.append(c.getAtomSequence());

            }

            return seq.toString();

        } catch (StructureException ex){
            System.err.println("Could not parse domain definitions for " + ecodId);
            return "";
        }



    }
}
