package alluxio.wire;

import com.google.common.base.MoreObjects;

public class ChunkResponse {

	private byte [] signature;
	private byte [] content;
	
	
	public byte[] getSignature() {
		return signature;
	}
	public void setSignature(byte[] signature) {
		this.signature = signature;
	}
	public byte[] getContent() {
		return content;
	}
	public void setContent(byte[] content) {
		this.content = content;
	}
	
	@Override
	  public String toString() {
	    return MoreObjects.toStringHelper(this)
	        .add("signature", signature)
	        .add("content", content)
	        .toString();
	  }
	
	
}
