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
import org.biojava.nbio.structure.ecod.EcodDatabase;
import org.biojava.nbio.structure.ecod.EcodDomain;
import org.biojava.nbio.structure.ecod.EcodFactory;
import org.biojava.spark.ClusterSequences;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import scala.Tuple2;
import scala.Tuple5;

import java.io.*;
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


    public static void main(String[] args){

        AtomCache cache;
        if ( args.length > 0){
            cache = new AtomCache(args[0]);
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

        ecodDomains = ecodDomains.sample(false,0.001);

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
        ClusterSequences cluster = new ClusterSequences();

        JavaRDD<Tuple5<String,String,Float,Float,Float>> results = cluster.clusterSequences(sc,sequences);

        JavaRDD hits = results.filter(t -> t._3() > MIN_OVERLAP && t._4() > MIN_OVERLAP && t._5() > MIN_PERCID);

        hits = hits.repartition(1);

        try {
            File outFile = File.createTempFile("seq_cluster","txt");
            hits.saveAsTextFile(outFile.getAbsolutePath());
            System.out.println("Wrote to " + outFile.getAbsolutePath());

        } catch (IOException e) {
            e.printStackTrace();
        }
        long timeE = System.currentTimeMillis();
        System.out.println("clustering took " + (timeE-timeS)/1000 + " sec. ");

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