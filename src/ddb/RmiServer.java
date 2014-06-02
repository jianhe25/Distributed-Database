package ddb;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;


public class RmiServer extends UnicastRemoteObject implements RmiService {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5555L;
	private int siteID;	
	private Connection con;
	private Statement statement;
	public ArrayList<Table> resultTables;
	private RmiService[] rmiClients;
	
	final static private int[] registryPorts = new int[]{-1, 8001, 8002, 8003, 8004};
	final static private String[] registryNames = new String[]{"null", 
		"rmi://127.0.0.1:8001/ddbService", 
		"rmi://127.0.0.1:8002/ddbService", 
		"rmi://127.0.0.1:8003/ddbService", 
		"rmi://127.0.0.1:8004/ddbService"};
	static Registry[] registry = new Registry[5];
			
	@SuppressWarnings("unused")
	private RmiServer() throws RemoteException {		
	}
	
	static public void stopServers() throws RemoteException {
		for (int i = 1; i <= 4; ++i)
			try {
				registry[i].unbind(registryNames[i]);
			} catch (NotBoundException e) {
				e.printStackTrace();
			}
	}
	
	static public void runServers() throws RemoteException {
		RmiServer[] rmiServers = new RmiServer[5];
		try {
			for (int i = 1; i <= 4; ++i){
				rmiServers[i] = new RmiServer(i);
				registry[i] = LocateRegistry.createRegistry(registryPorts[i]);
				registry[i].rebind(registryNames[i], rmiServers[i]);
			}				
			for (int i = 1; i <= 4; ++i)
				rmiServers[i].initClients();			
		} catch (RemoteException e) {
//			e.printStackTrace();
			System.err.println("start server failed");
		}
		System.out.println("==============Servers are now online===============");
	}
	
	public static RmiService[] getClients() throws RemoteException {
		Registry[] registry = new Registry[5];
		RmiService[] rmiClients = new RmiService[5];
		try {
			for (int i = 1; i <= 4; ++i) {
				registry[i] = LocateRegistry.getRegistry(registryPorts[i]); 
				rmiClients[i] = (RmiService)registry[i].lookup(registryNames[i]);
			}				
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		return rmiClients;
	}
	
	public void initClients() throws RemoteException {		
		this.rmiClients = RmiServer.getClients();
	}
	
	
	public RmiServer(int siteID) throws RemoteException {
		super();
		this.siteID = siteID;
		resultTables = new ArrayList<Table>();
		SitePartitioner sitePartitioner = new SitePartitioner();
		try {
			/***
			 * server use 1 2 3 4 to be the site, they don't use 0 and what is more,
			 * in the fragmantation, we do not use 0 as well 
			 */
			DriverManager.registerDriver(new com.mysql.jdbc.Driver ());
			con = DriverManager.getConnection(
					sitePartitioner.hostURLs[this.siteID], 
					sitePartitioner.username,
					sitePartitioner.password);
			statement = con.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	

	@Override
	public TableMeta select(String tableName, String whereClause) throws RemoteException {	
		System.out.println("---------start select--------- at " + siteID);
		long time1 = System.currentTimeMillis();
		ResultSet resultSet;
		Table resultTable = new Table();
		try {
			String q = "SELECT * FROM " + tableName + " " + whereClause + ";";
			System.out.println(q);
			resultSet = statement.executeQuery(q);
			ResultSetMetaData metaData = resultSet.getMetaData();
			int colomnNum = metaData.getColumnCount();
					
			for (int i = 0; i < colomnNum; ++i) {
				resultTable.addColumnName(tableName + "." + metaData.getColumnName(i+1));
			}			
			int numRow = 0;
			while (resultSet.next()) {
				ArrayList<String> row = new ArrayList<String>();
				for (int i = 0; i < colomnNum; ++i) {
					row.add(resultSet.getString(i+1));
				}
				resultTable.addRow(row);	
				if (++numRow > 50000)
					break;
			}							
			resultTables.add(resultTable);
		} catch (SQLException e) {
			e.printStackTrace();
		}		
		resultTable.meta = new TableMeta(resultTable.numRow(), 
										this.siteID, 
										resultTables.size() - 1, 
										resultTable.columnNames);
		long time2 = System.currentTimeMillis();
		System.out.println("---------finish select---------" + 
			(time2 - time1) + "ms " + 
			" site " + this.siteID  + 
			" numRows " + resultTable.numRow() + "\n");
		
		return resultTable.meta;		
	}
	
	@Override
	public TableMeta project(int tableID, String[] columnNames) throws RemoteException {	
		// For * selection, return full table directly
		if (columnNames.length == 1 && columnNames[0].equals("*")) {
			return resultTables.get(tableID).meta;
		}
		
		Table table = resultTables.get(tableID);
		HashSet<String> columnNameSet = new HashSet<String>();
		for (String columnName : columnNames)
			columnNameSet.add(columnName);
		
		boolean[] keepColumn = new boolean[table.columnNames.size()];
		for (int i = 0; i < keepColumn.length; ++i) keepColumn[i] = false;
		
		ArrayList<String> newColumnNames = new ArrayList<String>(); 
		for (int i = 0; i < table.columnNames.size(); ++i) {
			if (columnNameSet.contains(table.columnNames.get(i))) {
				keepColumn[i] = true;
				newColumnNames.add(table.columnNames.get(i));
			}
		}
		table.columnNames = newColumnNames;
		
		for (int i = 0; i < table.numRow(); ++i) {
			ArrayList<String> row = table.matrix.get(i);
			ArrayList<String> newRow = new ArrayList<String>();
			for (int j = 0; j < row.size(); ++j)
				if (keepColumn[j]) {
					newRow.add(row.get(j));
				}
			table.matrix.set(i, newRow);
		}		
		return table.meta;
	}
	
	@Override
	public TableMeta union(TableMeta tableMeta1, TableMeta tableMeta2) throws RemoteException {
		Table table1 = null;
		Table table2 = null;
		assert(tableMeta1 != null);
		assert(tableMeta2 != null);
		
		if (tableMeta1.siteID == this.siteID) {
			System.out.println("idInLocalSite = " + tableMeta1.idInLocalSite);
			table1 = resultTables.get(tableMeta1.idInLocalSite);
		} else {
			table1 = rmiClients[tableMeta1.siteID].getTableByID(tableMeta1.idInLocalSite);
		}
		
		if (tableMeta2.siteID == this.siteID) {
			table2 = resultTables.get(tableMeta2.idInLocalSite);
		} else {
			table2 = rmiClients[tableMeta2.siteID].getTableByID(tableMeta2.idInLocalSite);			
		}
		
		if (table1.columnNames.size() != table2.columnNames.size()) {			
			throw new RemoteException();
		}
		for (int i = 0; i < table1.columnNames.size(); ++i) 
			if (!table1.columnNames.get(i).equals(table2.columnNames.get(i))) {
//				System.err.println("t1 = " + table1.columnNames.toString());
//				System.err.println("t2 = " + table2.columnNames.toString());
				throw new RemoteException();
			}
				
		// Ensure table1 is large table, and move table2 to table1
		if (table1.numRow() < table2.numRow()) {
			Table tempTable = table2;
			table2 = table1;
			table1 = tempTable;
		}
				
		for (ArrayList<String> row : table2.matrix) {
			table1.matrix.add(row);
		}
		
		table1.meta.numRows = table1.numRow();		
		if (table1.meta.siteID != this.siteID) {
			resultTables.add(table1);
			table1.meta.siteID = this.siteID;
			table1.meta.idInLocalSite = resultTables.size() - 1;
		}
		return table1.meta;
	}
	
	@Override
	public TableMeta join(TableMeta tableMeta1, TableMeta tableMeta2, 
					 String keyColumn1, String keyColumn2) throws RemoteException {
		Table table1 = null;
		Table table2 = null;
		
		if (tableMeta1.siteID == this.siteID) {
			table1 = resultTables.get(tableMeta1.idInLocalSite);
		} else {
			table1 = rmiClients[tableMeta1.siteID].getTableByID(tableMeta1.idInLocalSite);
		}
		
		if (tableMeta2.siteID == this.siteID) {
			table2 = resultTables.get(tableMeta2.idInLocalSite);
		} else {
			table2 = rmiClients[tableMeta2.siteID].getTableByID(tableMeta2.idInLocalSite);			
		}
		
//		Ensure table1 is large table
		if (table1.numRow() < table2.numRow()) {
			Table tempTable = table2;
			table2 = table1;
			table1 = tempTable;
			String tempKey = keyColumn2;
			keyColumn2 = keyColumn1;
			keyColumn1 = tempKey;
		}
		
		int id1 = -1, id2 = -1;
		for (int i = 0; i < table1.columnNames.size(); ++i)
			if (table1.columnNames.get(i).equals(keyColumn1)) {
				id1 = i;
				break;
			}
		for (int i = 0; i < table2.columnNames.size(); ++i)
			if (table2.columnNames.get(i).equals(keyColumn2)) {
				id2 = i;
				break;
			}
		
//		System.out.println("id = " + id1 + " " + id2);
		if (id1 == -1 || id2 == -1) {
			if (id1 == -1)
				System.err.println("keyColumn1: " + keyColumn1 + " not exist!");
			if (id2 == -1)
				System.err.println("keyColumn2: " + keyColumn2 + " not exist!");
			throw new RemoteException();
		}
		
		Hashtable<String, ArrayList<Integer>> keys1 = new Hashtable<String, ArrayList<Integer>>(); 
		// Build table
		for (int i = 0; i < table1.matrix.size(); ++i) {
			ArrayList<Integer> list = keys1.get(table1.matrix.get(i).get(id1));
			if (list == null) 
				list = new ArrayList<Integer>();							
			list.add(i);			
			keys1.put(table1.matrix.get(i).get(id1), list);
		}
	
		Table newTable = new Table();	
		for (String columnName : table1.columnNames) { 
			newTable.columnNames.add(columnName);
		}		
		for (String columnName : table2.columnNames) 
		if (!columnName.equals(keyColumn2)) {
			newTable.columnNames.add(columnName);
		}		
		
		for (int i = 0; i < table2.matrix.size(); ++i) {			
			ArrayList<String> row2 = table2.matrix.get(i);			
			String key2 = row2.get(id2);
			ArrayList<String> newRow = new ArrayList<String>();
			if (keys1.containsKey(key2)) {				
				ArrayList<Integer> list = keys1.get(key2);
				for (Integer id : list) {
					newRow = table1.matrix.get(id);
					for (int j = 0; j < row2.size(); ++j)
						if (j != id2) {
							newRow.add(row2.get(j));
						}
					newTable.matrix.add(newRow);
				}								
			}
		}
		
		resultTables.add(newTable);
		newTable.meta = new TableMeta(newTable.numRow(), this.siteID, 
				resultTables.size()-1, newTable.columnNames);
		return newTable.meta;
	}
	
	@Override
	public Table getTableByID(int id) throws RemoteException {
		return resultTables.get(id);		
	}
	
	@Override
	public TableMeta selectBySimilarity(TableMeta tableMeta, 
			String columnName,
			String query, 
			double threshold, 
			String similarType) throws RemoteException {
		
		MyLogger.log("Start select by similarity");
		Table table = null;
		if (tableMeta.siteID == this.siteID) {
			table = resultTables.get(tableMeta.idInLocalSite); 
		} else {
			try {
				table = rmiClients[tableMeta.siteID].getTableByID(tableMeta.idInLocalSite);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}		
		int colIndex = -1;
		for (int i = 0; i < table.columnNames.size(); ++i)
			if (table.columnNames.get(i).equals(columnName)) {
				colIndex = i;
				break;
			}
		
		Table resultTable = new Table();
		if (similarType.equals("ED")) {
			for (ArrayList<String> row : table.matrix) {			
				if (verifyEditDistance(row.get(colIndex), query, (int)threshold)) {
					resultTable.addRow(row);
				}
			}
		} else if (similarType.equals("JACCARD")) {
			for (ArrayList<String> row : table.matrix) {			
				if (verifyJaccard(row.get(colIndex), query, threshold)) {
					resultTable.addRow(row);
				}
			}
		}
		resultTable.columnNames = table.columnNames; 
				
		resultTables.add(resultTable);		
		resultTable.meta = new TableMeta(resultTable.numRow(), 
				this.siteID, 
				resultTables.size() - 1, 
				resultTable.columnNames);
		MyLogger.log(resultTable.meta.toString());
		return resultTable.meta;		
	}
	
	private boolean verifyJaccard(String a, String b, double threshold) {
		String[] tokensA = a.split(" ");
		String[] tokensB = b.split(" ");
		Arrays.sort(tokensA);
		Arrays.sort(tokensB);
		
		int overlap = 0;
		int j = 0;
		for (int i = 0; i < tokensA.length; ++i) {
			while (j < tokensB.length && tokensA[i].compareTo(tokensB[j]) > 0) 
				++j;
			if (j < tokensB.length) {
				if (tokensA[i].equals(tokensB[j])) {
					overlap++;
					++j;
				}
			}
		}
		
		return (double)overlap / (tokensA.length + tokensB.length) >= threshold;
	}

	private boolean verifyEditDistance(String a, String b, int dist) {
		int LIMIT = dist + 1;
		int[][] dp = new int[a.length()+1][b.length()+1];
		for (int i = 0; i < dp.length; ++i)
			for (int j = 0; j < dp[i].length; ++j)
				dp[i][j] = LIMIT;
		
		dp[0][0] = 0;
		for (int i = 0; i < a.length(); ++i) {
			int l = Math.max(0, i - dist);
			int r = Math.min((int)b.length()-1, i+dist);
			boolean isUpdate = false;
			for (int j = l; j <= r; ++j) 
			if (dp[i][j] != LIMIT) {
				dp[i][j+1] = Math.min(dp[i][j+1], dp[i][j]+1);
				dp[i+1][j] = Math.min(dp[i+1][j], dp[i][j]+1);
				if (a.charAt(i) != b.charAt(j)) 
					dp[i+1][j+1] = Math.min(dp[i+1][j+1], dp[i][j] + 1);
				else 
					dp[i+1][j+1] = Math.min(dp[i+1][j+1], dp[i][j]);
				isUpdate = true;
			}
			if (!isUpdate) {
				return false;
			}
		}
		return dp[a.length()][b.length()] <= dist;
	}
}







