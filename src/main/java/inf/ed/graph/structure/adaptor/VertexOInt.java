package inf.ed.graph.structure.adaptor;

import inf.ed.graph.structure.OrthogonalEdge;
import inf.ed.graph.structure.OrthogonalVertex;

import java.io.Serializable;

/**
 * Orthogonal Vertex with integer label.
 * 
 * @author yecol
 *
 */
public class VertexOInt extends OrthogonalVertex implements Serializable {

	private static final long serialVersionUID = 1L;
	int attr;

	public VertexOInt(int id) {
		this.id = id;
	}

	public VertexOInt(VertexOInt copy, boolean copyEdge) {
		this.id = copy.id;
		this.attr = copy.attr;
	}

	public VertexOInt(String line) {

		String[] eles = line.split("\t");
		this.id = Integer.parseInt(eles[0].trim());
		this.attr = Integer.parseInt(eles[1].trim());

	}

	public VertexOInt(int id, int attr) {
		this.id = id;
		this.attr = attr;
	}

	public VertexOInt(int id, int attr, OrthogonalEdge firstin, OrthogonalEdge firstout) {
		this.id = id;
		this.attr = attr;
		this.firstin = firstin;
		this.firstout = firstout;
	}

	public int getAttr() {
		return this.attr;
	}

	public void setAttr(int attr) {
		this.attr = attr;
	}

	@Override
	public int hashCode() {
		int result = String.valueOf(this.getID()).hashCode();
		result = 29 * result + String.valueOf(attr).hashCode();
		return result;
	}

	@Override
	public boolean equals(Object other) {
		final VertexOInt v = (VertexOInt) other;
		return v.getID() == this.getID();
	}

	public boolean match(Object other) {
		final VertexOInt o = (VertexOInt) other;
		return o.attr == this.attr;
	}

	@Override
	public String toString() {
		return "VertexIntLO [ID=" + this.id + ", attr=" + this.attr + "]";
	}

	@Override
	public OrthogonalVertex copyWithoutEdge() {
		return new VertexOInt(this, false);
	}
}
