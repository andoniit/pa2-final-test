import java.io.Serializable;

public class RES implements Serializable {

	private int responseCode;
	private Object responseData;
	
	public int getRespCd() {
		return responseCode;
	}

	public void setRespCd(int responseCode) {
		this.responseCode = responseCode;
	}

	public Object getRespData() {
		return responseData;
	}

	public void setRespData(Object responseData) {
		this.responseData = responseData;
	}
}
