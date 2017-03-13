package inf.ed.graph.structure.adaptor;

import inf.ed.graph.structure.Vertex;

import java.io.Serializable;
import java.util.Map;

/**
 * Vertex with integer label.
 * 
 * @author yecol
 *
 */
public class VertexInt implements Vertex, Serializable {

	private static final long serialVersionUID = 1L;
	int ID;
	int attr;

	public VertexInt(int ID, int attr) {
		this.ID = ID;
		this.attr = attr;
	}

	public VertexInt(String line) {

		String[] eles = line.split("\t");
		this.ID = Integer.parseInt(eles[0].trim());
		this.attr = Integer.parseInt(eles[1].trim());

	}

	public VertexInt(Map<String, Object> map, String vertexLabel) {
		this.ID = (int) map.get(vertexLabel + ".id");
		this.attr = (int) map.get(vertexLabel + ".label");
	}

	public int getID() {
		return this.ID;
	}

	public int getAttr() {
		return attr;
	}

	public boolean match(Object other) {
		final VertexInt o = (VertexInt) other;
		return o.attr == this.attr;
	}

	@Override
	public int hashCode() {
		int result = String.valueOf(this.getID()).hashCode();
		result = 29 * result + String.valueOf(attr).hashCode();
		return result;
	}

	@Override
	public boolean equals(Object other) {
		final VertexInt v = (VertexInt) other;
		return v.getID() == this.getID();
	}

	@Override
	public String toString() {
		return "VertexIntL [ID=" + ID + ", attr=" + attr + "]";
	}
}
