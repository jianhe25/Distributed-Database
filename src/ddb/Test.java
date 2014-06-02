package ddb;
import java.util.ArrayList;

import ddb.SQLProcessor;

public class Test {
	public static void main(String[] args) {
		SQLProcessor processor = new SQLProcessor();
		
		ArrayList<String> sqlCommands = new ArrayList<String>();
		sqlCommands.add("SELECT customer.name FROM customer");
				
		for (String command : sqlCommands) { 
			processor.execute(command);
		}
	}
}
