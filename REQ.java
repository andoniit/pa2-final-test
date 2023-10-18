import java.io.Serializable;

public class REQ implements Serializable {
	
	private String requestType;
	private Object requestData;
	
	public String getReqT() {
		return requestType;
	}
	public void setReqType(String requestType) {
		this.requestType = requestType;
	}
	public Object getReqData() {
		return requestData;
	}
	public void setReqData(Object requestData) {
		this.requestData = requestData;
	}
	
}
