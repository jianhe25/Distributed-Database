package ddb;

import java.io.Serializable;
import java.util.ArrayList;

public class TableMeta implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 9999L;
	
	public int numRows;
	public int siteID;
	public int idInLocalSite;
	public ArrayList<String> columnNames; 
	public TableMeta(int _numRows, int _siteID, 
			int _idInLocalSite, ArrayList<String> _columnNames) {
		// TODO Auto-generated constructor stub
		numRows = _numRows;
		siteID = _siteID;
		idInLocalSite = _idInLocalSite;
		columnNames = _columnNames;
	}
	public String toString() {
		String content = "";
		content = "numRows: " + numRows + "\n" +
				  "siteID: " + siteID + "\n" +
				  "idInLocalSite: " + idInLocalSite + "\n" + 
				  "columnNames: " + columnNames + "\n";
		return content;
	}
}
