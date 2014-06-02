package ddb;

public class MyLogger {	
	public static void log(String message) {
		String fullClassName = Thread.currentThread().getStackTrace()[2].getClassName();            
	    String className = fullClassName.substring(fullClassName.lastIndexOf(".") + 1);
	    String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
	    int lineNumber = Thread.currentThread().getStackTrace()[2].getLineNumber();
	
	    System.out.println("[INFO] " + className + "." + methodName + "()#" + lineNumber + " : " + message);
	}
	
	public static void err(String message) {
		String fullClassName = Thread.currentThread().getStackTrace()[2].getClassName();            
	    String className = fullClassName.substring(fullClassName.lastIndexOf(".") + 1);
	    String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
	    int lineNumber = Thread.currentThread().getStackTrace()[2].getLineNumber();
	
	    System.out.println("[ERR] " + className + "." + methodName + "()#" + lineNumber + " : " + message);
	}
}
