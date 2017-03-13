package inf.ed.grape.interfaces;

import java.io.Serializable;

public class Message<T> implements Serializable {

	private static final long serialVersionUID = -6215563083195145662L;

	private int sourcePartitionID;

	private int destinationVertexID;

	private T content;

	public Message(int fromPartitionID, int destination, T content) {
		super();
		this.sourcePartitionID = fromPartitionID;
		this.destinationVertexID = destination;
		this.content = content;
	}

	public int getSourcePartitionID() {
		return this.sourcePartitionID;
	}

	public int getDestinationVertexID() {
		return this.destinationVertexID;
	}

	public T getContent() {
		return this.content;
	}

	@Override
	public String toString() {
		return "Message [" + content + "] -> " + destinationVertexID;
	}
}
