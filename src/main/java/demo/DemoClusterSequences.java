package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureException;
import org.biojava.nbio.structure.align.util.AtomCache;
import org.biojava.nbio.structure.align.util.UserConfiguration;
import org.biojava.nbio.structure.ecod.EcodDatabase;
import org.biojava.nbio.structure.ecod.EcodDomain;
import org.biojava.nbio.structure.ecod.EcodFactory;
import org.biojava.spark.ClusterSequences;
import org.biojava.spark.function.PrintClusterInfo;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import scala.Tuple2;
import scala.Tuple5;

import java.io.*;
import java.util.Iterator;
import java.util.List;


/**
 * Created by andreas on 4/30/16.
 */
public class DemoClusterSequences implements Serializable{

    private static final int NR_THREADS = Runtime.getRuntime().availableProcessors();
    private static final int TASKS_PER_THREAD = 3;
    private static final int BATCH_SIZE=100;

    private static final float MIN_OVERLAP = .9f;
    private static final float MIN_PERCID  = .4f;

    // break up the calculation in 10 fractions
    // each fraction will have the all vs. all calculation performed
    // and clustering will be merged
    private static final int nrFractions = 10;

    public static void main(String[] args){

        long timeS = System.currentTimeMillis();
        AtomCache cache;
        if ( args.length > 0){
            System.setProperty("PDB_DIR", args[0]);
            UserConfiguration config = new UserConfiguration();
            cache = new AtomCache(config);
        } else {
            cache  = new AtomCache();
        }

        EcodDatabase database = EcodFactory.getEcodDatabase();

        try {

            List<EcodDomain> domains = database.getAllDomains();

            JSONArray data = new JSONArray();
            int count = 0;
            for ( EcodDomain domain : domains){
                count ++;
                JSONObject jo = new JSONObject();
                jo.put("domainId",domain.getDomainId());
                data.put(jo);
            }

            String json = data.toString();

            File domainsFile = File.createTempFile("domains","json");

            try (PrintStream out = new PrintStream(new FileOutputStream(domainsFile))) {
                out.print(json);
            }

            System.out.println("wrote ECOD domains to " + domainsFile);

            DemoClusterSequences me = new DemoClusterSequences();

            me.clusterAllEcodDomains(domainsFile);


        } catch (Exception e) {
            e.printStackTrace();
        }

        long timeE = System.currentTimeMillis();

        System.err.println("TOTAL TIME: " + ( timeE-timeS)/100 + " sec.");

    }



    private void clusterAllEcodDomains(File jsonFile) {
        SparkConf conf = new SparkConf()
                .setMaster("local[" + NR_THREADS+"]")
                .setAppName(this.getClass().getSimpleName())
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer");


        // read the PDB to ECOD mapping file

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy");

        sqlContext.setConf("spark.sql.parquet.filterPushdown", "true");

        DataFrame ecodDomains = sqlContext.read().json(jsonFile.getAbsolutePath());

        ecodDomains.printSchema();

        ecodDomains = ecodDomains.limit(50);

        //ecodDomains = ecodDomains.sample(false,0.0001);

        ecodDomains.show();

        System.out.println("Retrieving " + ecodDomains.count() + " ECOD domain sequences...");

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


        long timeS = System.currentTimeMillis();
        ClusterSequences cluster = new ClusterSequences(nrFractions, MIN_OVERLAP, MIN_OVERLAP,MIN_PERCID );

        JavaPairRDD<String,Iterable<String>> results = cluster.clusterSequences(sc,sequences,5);

        int totalCount = 0;
        if ( results != null && ! results.isEmpty())
            results.foreach(t->
                System.out.println("FINAL CLUSTER: " + t)
              );


        List<Tuple2<String,Iterable<String>>>  clusters = results.collect();

        PrintClusterInfo printClusterInfo = new PrintClusterInfo(clusters);

        sequences.foreach(printClusterInfo);



//        try {
//            File outFile = File.createTempFile("seq_cluster","txt");
//            hits.saveAsTextFile(outFile.getAbsolutePath());
//            System.out.println("Wrote to " + outFile.getAbsolutePath());
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        long timeE = System.currentTimeMillis();
        long runTime = (timeE- timeS);
        System.out.println("clustering took " + (runTime/1000) + " sec. ");
        long n = ecodDomains.count();
        long combinations = n * (n-1) / 2;

        System.out.println("Average time per comparison: " + (runTime / combinations) + " ms.");

    }




    public  String getSequence(String ecodId) throws IOException, StructureException {

        AtomCache cache = new AtomCache();

        //cache.setUseMmtf(true);

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
