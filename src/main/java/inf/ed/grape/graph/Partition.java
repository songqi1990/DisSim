package inf.ed.grape.graph;

import inf.ed.grape.util.IO;
import inf.ed.graph.structure.Edge;
import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.OrthogonalGraph;
import inf.ed.graph.structure.Vertex;
import inf.ed.graph.structure.adaptor.VertexOString;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.Serializable;

/**
 * Data structure of partition, including a graph fragment and vertices with
 * crossing edges.
 * 
 * @author yecol
 *
 */

public class Partition implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private int partitionID;

	Graph<? extends Vertex, ? extends Edge> graph;

	public IntSet outgoingVertices;
	public IntSet incomingVertices;

	public Partition(int partitionID) {
		this.partitionID = partitionID;
	}

	public boolean loadPartitionDataFromEVFile(String filePath) {
		this.graph = new OrthogonalGraph<VertexOString>(VertexOString.class);
		this.graph.loadGraphFromVEFile(filePath);

		return true;
	}

	public int getPartitionID() {
		return partitionID;
	}

	public Graph<? extends Vertex, ? extends Edge> getGraph() {
		return this.graph;
	}

	public void addOutgoingVertex(int vertexID) {
		this.outgoingVertices.add(vertexID);
	}

	public void setOutgoingVerticesSet(IntSet outgoingVertices) {
		this.outgoingVertices = outgoingVertices;
	}

	// public boolean isOutgoingVertex(int vertexID) {
	// return this.outgoingVertices.contains(vertexID);
	// }

	public boolean isVirtualVertex(int vertexID) {
		return this.outgoingVertices.contains(vertexID);
	}

	public boolean isIncomingVertex(int vertexID) {
		return this.incomingVertices.contains(vertexID);
	}

	public void loadOutgoingVerticesFromFile(String filePath) {
		this.outgoingVertices = IO.loadIntSetFromFile(filePath + ".o");
	}

	public void loadIncomingVerticesFromFile(String filePath) {
		this.incomingVertices = IO.loadIntSetFromFile(filePath + ".i");
	}

	public String getPartitionInfo() {
		String ret = "pID = " + this.partitionID + " | vertices = " + this.graph.vertexSize()
				+ " | edges = " + this.graph.edgeSize();
		if (outgoingVertices != null) {
			ret += " | ov = " + outgoingVertices.size();
		}
		if (incomingVertices != null) {
			ret += " | iv = " + incomingVertices.size();
		}
		return ret;
	}
}
