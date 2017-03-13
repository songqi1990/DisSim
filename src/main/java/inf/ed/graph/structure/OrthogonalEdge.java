package inf.ed.graph.structure;

import java.io.Serializable;

public class OrthogonalEdge implements Edge, Serializable {

	/**
	 * With the help of head link and tail link, it is easy to find an arc
	 * headed/tailed on a certain vertex.
	 */

	private static final long serialVersionUID = 1L;
	private OrthogonalVertex fromNode;
	private OrthogonalVertex toNode;
	private OrthogonalEdge hlink;// head link
	private OrthogonalEdge tlink;// tail link

	public OrthogonalEdge() {
	}

	public OrthogonalEdge(OrthogonalVertex fromNode, OrthogonalVertex toNode, OrthogonalEdge hlink,
			OrthogonalEdge tlink) {
		this.fromNode = fromNode;
		this.toNode = toNode;
		this.hlink = hlink;
		this.tlink = tlink;
	}

	public OrthogonalEdge GetHLink() {
		return this.hlink;
	}

	public OrthogonalEdge GetTLink() {
		return this.tlink;
	}

	public void SetFromNode(OrthogonalVertex fnode) {
		this.fromNode = fnode;
	}

	public void SetToNode(OrthogonalVertex tnode) {
		this.toNode = tnode;
	}

	public void SetHLink(OrthogonalEdge hlink) {
		this.hlink = hlink;
	}

	public void SetTLink(OrthogonalEdge tlink) {
		this.tlink = tlink;
	}

	public boolean equals(Object other) {
		if (other == null)
			return false;
		if (!(other instanceof OrthogonalEdge))
			return false;

		final OrthogonalEdge e = (OrthogonalEdge) other;
		if (!this.fromNode.equals(e.from()) || !this.toNode.equals(e.to())) {
			return false;
		}
		return true;
	}

	public int hashcode() {
		int result = String.valueOf(this.fromNode).hashCode()
				+ String.valueOf(this.toNode).hashCode();
		return result;
	}

	public OrthogonalVertex from() {
		return this.fromNode;
	}

	public OrthogonalVertex to() {
		return this.toNode;
	}

	public boolean match(Object o) {
		return true;
	}

}
