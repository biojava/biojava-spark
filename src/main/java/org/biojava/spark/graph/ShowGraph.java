package org.biojava.spark.graph;

import javax.swing.JFrame;
import javax.swing.SwingUtilities;
import java.util.Set;

import org.jgrapht.Graph;
import org.jgrapht.ListenableGraph;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.ListenableDirectedWeightedGraph;
import com.mxgraph.layout.mxCircleLayout;
import com.mxgraph.layout.mxIGraphLayout;
import com.mxgraph.swing.mxGraphComponent;

/**
 * A class to render a Graph given a list of label names
 * @author tenorsax
 * @author Yana Valasatava (adaptation)
 * @author Anthony Bradley
 *
 */
public class ShowGraph {

	private static Graph<Integer, DefaultWeightedEdge> graph;
	private static String[] names;

	/**
	 * Constructor requires the graph and the labels of the vertices.
	 * @param graph the input {@link Graph} object
	 * @param names the labels of the vertices
	 */
	public ShowGraph(Graph<Integer, DefaultWeightedEdge> graph, String[] names) {
		setGraph(graph);
		setNames(names);
	}

	public void showGraph(double threshold, boolean showEdgeLabels) {

		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				createAndShowGui(threshold, showEdgeLabels);
			}
		});
	}

	private static void createAndShowGui(double threshold, boolean showEdgeLabels) {

		JFrame frame = new JFrame("NewGraph");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		ListenableGraph<String, MyEdge> g = buildGraph(threshold, showEdgeLabels);
		JGraphXAdapter<String, MyEdge> graphAdapter = 
				new JGraphXAdapter<String, MyEdge>(g);

		mxIGraphLayout layout = new mxCircleLayout(graphAdapter);
		layout.execute(graphAdapter.getDefaultParent());

		frame.add(new mxGraphComponent(graphAdapter));

		frame.pack();
		frame.setLocationByPlatform(true);
		frame.setVisible(true);
	}

	public static class MyEdge extends DefaultWeightedEdge {

		private static final long serialVersionUID = -2329441316446644067L;

		@Override
		public String toString() {
			return String.valueOf(getWeight());
		}
	}

	public static ListenableGraph<String, MyEdge> buildGraph(double threshold, boolean showEdgeLabels) {

		ListenableDirectedWeightedGraph<String, MyEdge> g = 
				new ListenableDirectedWeightedGraph<String, MyEdge>(MyEdge.class);

		Graph<Integer, DefaultWeightedEdge> inGraph = getGraph();		
		Set<DefaultWeightedEdge> edgeSet = inGraph.edgeSet();

		for (DefaultWeightedEdge e : edgeSet) {

			int sourceV = inGraph.getEdgeSource(e);
			String v1 = names[sourceV];

			int targetV = inGraph.getEdgeTarget(e);
			String v2 = names[targetV];

			g.addVertex(v1);
			g.addVertex(v2);

			double w = inGraph.getEdgeWeight(e);
			if(w>threshold) {
				MyEdge edge = g.addEdge(v1, v2);
				if(showEdgeLabels){
					g.setEdgeWeight(edge, w);
				}
			}

		}

		return g;
	}

	public static Graph<Integer, DefaultWeightedEdge> getGraph() {
		return graph;
	}

	public static void setGraph(Graph<Integer, DefaultWeightedEdge> graph) {
		ShowGraph.graph = graph;
	}

	public String[] getNames() {
		return names;
	}

	public void setNames(String[] names) {
		ShowGraph.names = names;
	}
}
