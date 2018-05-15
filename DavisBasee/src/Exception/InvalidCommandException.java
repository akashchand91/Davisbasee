package Exception;

public class InvalidCommandException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String message;
	public InvalidCommandException(String msg){
		message = msg;
	}
	public String getMessage() {
		return message;
	}
}
