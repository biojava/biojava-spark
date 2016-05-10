package org.biojava.spark.function;

import org.apache.spark.api.java.function.VoidFunction;
import org.jgrapht.Graph;
import scala.Tuple2;

/**
 * Created by ap3 on 09/05/2016.
 */
public class BuildUndirectedGraph implements VoidFunction<Tuple2<String, String>> {

    Graph graph;

    public  BuildUndirectedGraph(Graph graph){

        this.graph = graph;

    }

    @Override
    public void call(Tuple2<String, String> t) throws Exception {
        //if ( ! graph.containsVertex(t._1))
        graph.addVertex(t._1);

        //if ( ! graph.containsVertex(t._2))
        graph.addVertex(t._2);

        //if (! graph.containsEdge(t._1,t._2)) {
        graph.addEdge(t._1,t._2);
        //}


        //System.out.println(graph.edgeSet().size());
    }
}
