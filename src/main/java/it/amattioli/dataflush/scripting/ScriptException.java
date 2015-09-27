package it.amattioli.dataflush.scripting;

public class ScriptException extends RuntimeException {

	public ScriptException(int lineNumber, String message) {
		super("Syntax error in line "+lineNumber+": "+message);
	}
	
	public ScriptException(int lineNumber, Exception e) {
		this(lineNumber, getExceptionMsg(e));
	}
	
	private static String getExceptionMsg(Exception e) {
		Throwable targetException = getRootCause(e);
		String msg = targetException.getMessage();
		if (msg == null) {
			msg = targetException.toString();
		}
		return msg;
	}
	
	private static Throwable getRootCause(Throwable e) {
		if (e.getCause() != null) {
			return getRootCause(e.getCause());
		} else {
			return e;
		}
	}
}
