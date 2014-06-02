package ddb;

import java.util.ArrayList;

/**
 * 
 * @author hejian
 *
 */
class SQLWhere {
	String head;
	String tail;
	String operator;
	String table1; 
	String table2;
	String attribute1;
	String attribute2;
	String type;
	
	double threshold;
	/**
	 * Constructor for join and select
	 * @param _head
	 * @param _tail
	 * @param _operator
	 */
	public SQLWhere(String _head, String _tail, String _operator) {
		head = _head;
		tail = _tail;
		operator = _operator;
		table1 = StringCleaner.clean(head.substring(0, head.indexOf(".")));
		attribute1 = StringCleaner.clean(head.substring(head.indexOf(".")+1));			
		if (tail.contains(".")) {
			table2 = StringCleaner.clean(tail.substring(0, tail.indexOf(".")));
			attribute2 = StringCleaner.clean(tail.substring(tail.indexOf(".")+1));
			type = "join";
		} else {
			type = "select";
		}
	}
	
	/**
	 * Constructor for Jaccard and ED
	 * @param _head
	 * @param _tail
	 * @param _similarityType
	 * @param _threshold
	 */
	public SQLWhere(String _head, String _tail, String _similarityType, double _threshold) {
		head = _head;
		tail = _tail;
		type = _similarityType;
		table1 = StringCleaner.clean(head.substring(0, head.indexOf(".")));
		attribute1 = StringCleaner.clean(head.substring(head.indexOf(".")+1));
		threshold = _threshold;		
	}
	
	@Override
	public String toString() {
		if (type.equals("select")) {
			return table1 + "." + attribute1 + " " + operator + " " + tail;
		} else
		if (type.equals("join")) {
			return table1 + "." + attribute1 + " " + operator + " " + 
					table2 + "." + attribute2; 
		} else {
			return type + "(" + table1 + "." + attribute1 + ", " + tail + ") ~ " + threshold;   
		}
		
	}
	
	public String fullAttribute1() {
		return table1 + "." + attribute1;
	}
	
	public String fullAttribute2() {
		return table2 + "." + attribute2;
	}
	
	static public String UnionSelect(ArrayList<SQLWhere> selects) {
		String command = "";
		boolean isFirst = true;
		for (SQLWhere select : selects) 
		if (select.type.equals("join") || select.type.equals("select")) {
			if (!isFirst)
				command += " AND " + select.toString();
			else {
				command += " WHERE " + select.toString();
			}
			isFirst = false;
		}		
		return command;
	}
}
/**
 * 
 * @author hejian
 *
 * @format
 * select t1.a, t2.b, t3.c
 * from  t1, t2, t3
 * where 
 * 		t1.a = t2.b and
 * 		t1.b < t3.c and
 * 		t3.a > 1000 and
 * 		t1.a > 50
 *  
 * @constraint
 * 1. In select clause 
 * t1.a, t2.b, t3.c has to be presented in the joined table in where clause
 *   (by this constraint, we don't support Cartesian product)
 * 
 * 2. In where clause
 * Every attribute should has table name. Should be "table.attr"
 * 
 */
public class SQLCommand {
	public ArrayList<SQLWhere> selects = new ArrayList<SQLWhere>();
	public ArrayList<SQLWhere> joins = new ArrayList<SQLWhere>();
	
	public ArrayList<String> projects = new ArrayList<String>();	
	public ArrayList<String> froms = new ArrayList<String>(); 
	
	public String toString() {
		return 	"projects : " + projects.toString() + "\n" +
				"froms : " + froms.toString() + "\n" + 
				"selects : " + selects.toString() + "\n" + 
				"joins : " + joins.toString() + "\n";				
	}
}
