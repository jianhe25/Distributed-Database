package ddb;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



import ddb.Table;
import ddb.SQLTree;
import ddb.RmiServer;

/**
 * Parse SQL command
 * Build SQLtree
 * Execute SQLtree 
 * 
 * @author hejian
 *
 */
public class SQLProcessor {
	
	public SQLProcessor() {
		try {
			RmiServer.runServers();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param command
	 * 
	 * Our system has constraint on SQL command 
	 * see detail in class SQLCommand
	 * 
	 * @return SQLCommand
	 */
	public SQLCommand parse(String command) {
		
		int pos1 = command.indexOf("select");
		int pos2 = command.indexOf("from");
		int pos3 = command.indexOf("where");
				
		String projects = null; 
		String froms = null; 
		String wheres = null;
		projects = StringCleaner.clean( command.substring(pos1 + 6, pos2) );
		// if no where clause
		if (pos3 == -1) {
			froms = StringCleaner.clean( command.substring(pos2 + 4, command.length()-1) );
		} else {
			froms = StringCleaner.clean( command.substring(pos2 + 4, pos3) );
			wheres = StringCleaner.clean( command.substring(pos3 + 5, command.length()-1) );			
		}		
		
		SQLCommand sqlCommand = new SQLCommand();
		if (wheres != null)
			sqlCommand = parseWheres(wheres, sqlCommand);
		sqlCommand.projects = StringCleaner.cleanList(new ArrayList<String>(
				Arrays.asList(projects.split(","))));
		sqlCommand.froms = StringCleaner.cleanList(new ArrayList<String>(
				Arrays.asList(froms.split(","))));
		
		return sqlCommand;		
	}
	

	private SQLCommand parseWheres(String wheres, SQLCommand sqlCommand) {
		ArrayList<String> wheresList = 
				StringCleaner.cleanList(new ArrayList<String>(Arrays.asList(wheres.split("and"))));		
		ArrayList<SQLWhere> selects = new ArrayList<SQLWhere>(); 
		ArrayList<SQLWhere> joins = new ArrayList<SQLWhere>(); 
		for (int i = 0; i < wheresList.size(); ++i) {
			String where = wheresList.get(i);			
			if (where.startsWith("ED") || where.startsWith("JACCARD")) {
				String operator = findOperator(where);
				int operatorPos = where.indexOf(operator);
				String threshold = StringCleaner.clean(where.substring(operatorPos + 1));
				
				Pattern pattern = Pattern.compile("\\((.*?)\\)");
				Matcher matcher = pattern.matcher(where);
				matcher.find();				
				String content = matcher.group(1);
				String[] parts = content.split(",");
				String head = StringCleaner.clean(parts[0]);
				String tail = StringCleaner.clean(parts[1]);
				if (tail.startsWith("'") || tail.startsWith("\"")) {
					tail = tail.substring(1, tail.length()-1);
				}
				SQLWhere sqlWhere;
				if (where.startsWith("ED")) {
					sqlWhere = new SQLWhere(head, tail, "ED", Double.parseDouble(threshold));
				} else {
					sqlWhere = new SQLWhere(head, tail, "JACCARD", Double.parseDouble(threshold));
				}
				selects.add(sqlWhere);
			} else {
				String operator = findOperator(where);
				int operatorPos = where.indexOf(operator);
				String head = StringCleaner.clean(where.substring(0, operatorPos - 1));
				String tail = StringCleaner.clean(where.substring(operatorPos + 1));	
				if (tail.startsWith("'") || tail.startsWith("\"")) {
					tail = tail.substring(1, tail.length()-1);
				}
							
				SQLWhere sqlWhere = new SQLWhere(head, tail, operator);						
				if (sqlWhere.type.equals("join")) {				
					joins.add(sqlWhere);				
				} else {				
					selects.add(sqlWhere);
				}
			}
		}
		
		sqlCommand.joins = joins;
		sqlCommand.selects = selects;
		return sqlCommand;
	}
	
	private String findOperator(String requirement) {
		// TODO Auto-generated method stub
		if (requirement.contains("="))
			return "=";
		if (requirement.contains("<"))
			return "<";
		if (requirement.contains(">"))
			return ">";		
		return null;
	}

	
	/*
	 * @param: command
	 * @return: resultTable  
	 */
	public Table execute(String command) {
		SQLCommand sqlCommand = parse(command);
		SQLTree tree = new SQLTree();
		try {
			tree.buildTree(sqlCommand);
		} catch (Exception e){
			e.printStackTrace();
		}
		Table table = tree.execute();
		
		return table;
	}
}

