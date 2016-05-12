package org.biojava.spark.graph;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.jgrapht.Graph;
import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.alg.PrimMinimumSpanningTree;
import org.jgrapht.graph.AbstractBaseGraph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

import scala.Tuple3;


/**
 * @author Yana Valasatava
 *
 */
public class WeightedGraph {

	/**
	 * Builds a simple weighted graph
	 * @param data The input list of tuples each holds two indices and distance between them
	 */
	public static Graph<Integer, DefaultWeightedEdge> build(List<Tuple3<Integer, Integer, Double>> data) {

		// initialize a weighted graph
		Graph<Integer, DefaultWeightedEdge> graph = new SimpleWeightedGraph<Integer, DefaultWeightedEdge>(DefaultWeightedEdge.class);

		// add vertices and edges to the weighted graph
		for (Tuple3<Integer, Integer, Double> t : data) {

			int i = t._1();
			int j = t._2();
			double score = t._3();

			graph.addVertex(i);
			graph.addVertex(j);
			DefaultWeightedEdge e = graph.addEdge(i, j);
			((AbstractBaseGraph<Integer, DefaultWeightedEdge>) graph).setEdgeWeight(e, score);

		}
		return graph;
	}

	/**
	 * Builds a graph that contains a Minimum Spanning Tree
	 * @param graph The input weighted graph
	 */
	public static Graph<Integer, DefaultWeightedEdge> getMinimumSpanningTree(Graph<Integer, DefaultWeightedEdge> graph) {

		// get the edges of a Minimum Spanning Tree generated for a weighted graph
		PrimMinimumSpanningTree<Integer, DefaultWeightedEdge> tree = new PrimMinimumSpanningTree<Integer, DefaultWeightedEdge>(graph);
		Set<DefaultWeightedEdge> mspEdges = tree.getMinimumSpanningTreeEdgeSet();

		// build a new graph from the MST edges
		Graph<Integer, DefaultWeightedEdge> mspGraph = new SimpleWeightedGraph<Integer, DefaultWeightedEdge>(DefaultWeightedEdge.class);

		for (DefaultWeightedEdge e : mspEdges) {

			int sourceV = graph.getEdgeSource(e);
			int targetV = graph.getEdgeTarget(e);

			mspGraph.addVertex(sourceV);
			mspGraph.addVertex(targetV);

			DefaultWeightedEdge edge = graph.getEdge(sourceV, targetV);
			double w = graph.getEdgeWeight(edge);
			mspGraph.addEdge(sourceV, targetV, edge);
			((AbstractBaseGraph<Integer, DefaultWeightedEdge>) mspGraph).setEdgeWeight(edge, w);
		}

		return mspGraph;
	}

	/**
	 * Builds a path (root and branches) to traverse a graph 
	 * @param graph The input graph
	 */
	public static List<List<Integer>> traverse(Graph<Integer, DefaultWeightedEdge> graph) {

		// Get all data point in a graph
		Set<Integer> vSet = graph.vertexSet();
		List<Integer> verteces = new ArrayList<Integer>();
		verteces.addAll(vSet);

		// Pool of segments
		List<List<Integer>> segments = new ArrayList<List<Integer>>();

		// identify the most distant points in a graph: startVertex and endVertex

		BreadthFirstIterator<Integer, DefaultWeightedEdge> iteratorBack = new BreadthFirstIterator<Integer, DefaultWeightedEdge>(graph, verteces.get(0));
		int endVertex = 0;
		while (iteratorBack.hasNext()) {
			endVertex = iteratorBack.next();
		}

		BreadthFirstIterator<Integer, DefaultWeightedEdge> iteratorForth = new BreadthFirstIterator<Integer, DefaultWeightedEdge>(graph, endVertex);
		int startVertex = 0;
		while (iteratorForth.hasNext()) {
			startVertex = iteratorForth.next();
		}

		// get the shortest path between starting and ending vertices

		DijkstraShortestPath<Integer, DefaultWeightedEdge> pathFinder = new DijkstraShortestPath<Integer, DefaultWeightedEdge>(graph, startVertex, endVertex);
		List<DefaultWeightedEdge> path = pathFinder.getPathEdgeList();

		//======= get root ========

		ArrayList<Integer> segment = new ArrayList<Integer>();

		int source, target;

		int source1 = graph.getEdgeSource(path.get(0));
		int target1 = graph.getEdgeTarget(path.get(0));

		int source2 = graph.getEdgeSource(path.get(1));
		int target2 = graph.getEdgeTarget(path.get(1));

		if (target1 == source2 || target1 == target2) {
			source = source1;
			target = target1;
		}
		else {
			target = source1;
			source = target1;
		}

		segment.add(source);
		segment.add(target);

		verteces.remove(verteces.indexOf(source));
		verteces.remove(verteces.indexOf(target));

		for (int p=1; p < path.size(); p++) {

			int sourceVnext = graph.getEdgeSource(path.get(p));
			int targetVnext = graph.getEdgeTarget(path.get(p));

			if (sourceVnext != target) {
				target = sourceVnext;
				source = targetVnext;
			}
			else {
				target = targetVnext;
				source = sourceVnext;
			}
			segment.add(target);
			verteces.remove(verteces.indexOf(target));
		}

		segments.add(segment);

		// ======= get leafs ========
		ArrayList<Integer> leafs = new ArrayList<Integer>();
		for (Integer l : verteces){
			Set<DefaultWeightedEdge> egs = graph.edgesOf(l);
			if (egs.size() == 1) {
				leafs.add(l);
			}
		}

		// ======= get branches ========

		for (int leaf : leafs) {

			int min = Integer.MAX_VALUE;
			int anchor = -1;

			for (List<Integer> track : segments) {

				for (int t : track) {

					DijkstraShortestPath<Integer, DefaultWeightedEdge> pathFinderBranch = new DijkstraShortestPath<Integer, DefaultWeightedEdge>(graph, leaf, t);
					List<DefaultWeightedEdge> pathBranch = pathFinderBranch.getPathEdgeList();

					if (pathBranch.size() < min) {
						min = pathBranch.size();
						anchor = t;
					}	
				}
			}

			if (anchor != -1) {

				ArrayList<Integer> branch = new ArrayList<Integer>();

				DijkstraShortestPath<Integer, DefaultWeightedEdge> f = new DijkstraShortestPath<Integer, DefaultWeightedEdge>(graph, anchor, leaf);
				List<DefaultWeightedEdge> b = f.getPathEdgeList();

				if (b.size() == 1) {
					branch.add(anchor);
					branch.add(leaf);
				}
				else {
					branch.add(anchor);

					int start = graph.getEdgeSource(b.get(0));
					int end = graph.getEdgeTarget(b.get(0));

					if (anchor == start) {

						branch.add(end);
					}
					else {

						branch.add(start);
						end = start;
					}

					for (int q=1; q < b.size(); q++) {

						int s = graph.getEdgeSource(b.get(q));
						int e = graph.getEdgeTarget(b.get(q));

						if (s == end) {
							branch.add(e);
							end = e;
						}
						else {
							branch.add(s);
							end = s;
						}
					}

				}
				segments.add(branch);
			}	
		}
		return segments;
	}
}