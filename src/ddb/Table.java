package ddb;

import java.io.Serializable;
import java.util.ArrayList;
import ddb.TableMeta;


public class Table implements Serializable {
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 8888L;
	TableMeta meta;
	public ArrayList<String> columnNames = new ArrayList<String>();
	public ArrayList<ArrayList<String>> matrix = new ArrayList<ArrayList<String>>();
	
	public void addColumnName(String columnName) {	
		columnNames.add(columnName);
	}
	
	String getColumnName(int columnId) {
		return columnNames.get(columnId);
	}

	public void addRow(ArrayList<String> row) {
		matrix.add(row);
	}
	
	ArrayList<String> getRow(int rowId) {
		return matrix.get(rowId); 
	}

	public int numRow() {
		// TODO Auto-generated method stub
		return matrix.size();
	}

	public String toString() {
		StringBuilder text = new StringBuilder();
		for (int i = 0; i < columnNames.size(); ++i) 
			text.append(columnNames.get(i) + "\t");
		text.append("\n");
		for (int i = 0; i < matrix.size() && i < 10; ++i){
			ArrayList<String> row = matrix.get(i);
			for (int j = 0; j < row.size(); ++j) {
				text.append(row.get(j) + "\t");
			}
			text.append("\n");
		}
		return text.toString();
	}
}
