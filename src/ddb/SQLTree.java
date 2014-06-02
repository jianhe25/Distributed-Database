package ddb;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Hashtable;
import ddb.Table;

class SQLTreeNode {
	ArrayList<String> projects;	
	// "project", "select", "join"
	String type = null;
	SQLWhere join = null;
	ArrayList<SQLWhere> selects = new ArrayList<SQLWhere>();
	String tableName;
	
	int id;
	RmiServer[] rmiClients;
	
	public SQLTreeNode() {
		// TODO Auto-generated constructor stub
	}
	public SQLTreeNode(String _type) {
		type = _type;
	}
	
	SQLTreeNode farther, left, right;
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		String content = "node : " + id + " type : " + type + "\n";
		if (this.farther != null)	
			content += "farther = " + this.farther.id + "\n";
		if (this.left != null)
			content += "left = " + this.left.id + "\n";
		if (this.right != null)
			content += "right = " + this.right.id + "\n";
		if (this.projects != null)
			content += "project = " + projects.toString() + "\n";
		
		content += "select = " + selects.toString() + "\n";		
		if (this.join != null) {
			content += "join = " + join.toString() + "\n";
		}
		return content;
	}
}
public class SQLTree {
	public SQLTreeNode root;
	Hashtable<String, SQLTreeNode> leafTable = new Hashtable<String, SQLTreeNode>();
	ArrayList<SQLTreeNode> nodes = new ArrayList<SQLTreeNode>();	
	private RmiService[] rmiClients;

	public SQLTree() {		
		try {
			this.rmiClients = RmiServer.getClients();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public SQLTreeNode buildTree(SQLCommand sqlCommand) throws Exception  {
		for (String table : sqlCommand.froms) { 
			SQLTreeNode node = new SQLTreeNode();				
			leafTable.put(table, node);
			nodes.add(node);
			node.type = "select";
			node.tableName = table;
		}
		
		for (SQLWhere select : sqlCommand.selects) {
			if (leafTable.containsKey(select.table1)) {				
				SQLTreeNode node = leafTable.get(select.table1);	
				node.selects.add(select); // Would this change node in leafTable?				
			}
		}
		
		for (SQLWhere join : sqlCommand.joins) {
			String table1 = join.table1;
			String table2 = join.table2;
			SQLTreeNode node1 = this.findRootFromLeaf(table1);
			SQLTreeNode node2 = this.findRootFromLeaf(table2);
			SQLTreeNode node = new SQLTreeNode("join");
			node.join = join;
			nodes.add(node);
			node.left = node1;
			node.right = node2;
			node1.farther = node;
			node2.farther = node;
		}
		
		ArrayList<SQLTreeNode> rootNodes = new ArrayList<SQLTreeNode>();
		for (SQLTreeNode node : nodes)
			if (node.farther == null) {
				rootNodes.add(node);
			}
		
		if (rootNodes.size() != 1) {
			throw new Exception();
		}
		
		root = new SQLTreeNode();
		root.projects = sqlCommand.projects;		
		root.type = "project";		
		root.left = rootNodes.get(0);
		nodes.add(root);
		
		for (int i = 0; i < nodes.size(); ++i)
			nodes.get(i).id = i;		
		return root;
	}
	
	SQLTreeNode findRootFromLeaf(String tableName) {		
		SQLTreeNode node = leafTable.get(tableName);		
		while (node.farther != null) {
			node = node.farther;
		}
		return node;
	}
	
	
	TableMeta executeNode(SQLTreeNode node) {
		TableMeta tableMeta1 = null;
		TableMeta tableMeta2 = null;
		TableMeta tableMeta = null;
		MyLogger.log("node = " + node);
		if (node.left != null) {
			tableMeta1 = executeNode(node.left);
		}
		if (node.right != null) {
			tableMeta2 = executeNode(node.right);
		}
		
		if (node.type.equals("project")) {
			assert(tableMeta1 != null);
			try {
				System.out.println("tableMeta1 = " + tableMeta1);
				int siteID = tableMeta1.siteID;
				System.out.println("siteID = " + siteID);
				tableMeta = rmiClients[siteID].project(tableMeta1.idInLocalSite, 
						node.projects.toArray(new String[0]));
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if (node.type.equals("select")) {			
//			int[] siteIDs = new int[node.selects.size()];			
			ArrayList<ArrayList<SQLWhere>> selectsPerSite = new ArrayList<ArrayList<SQLWhere>>();
			for (int i = 0; i < 5; ++i)
				selectsPerSite.add(null);
			
			// TODO: fix later
			for (int i = 0; i < node.selects.size(); ++i) {
				SQLWhere select = node.selects.get(i);
				ArrayList<Integer> siteIDs = SitePartitioner.calcSiteFromSelect(select);
				for (int siteID : siteIDs) {
					if (selectsPerSite.get(siteID) == null)
						selectsPerSite.set(siteID, new ArrayList<SQLWhere>());
					selectsPerSite.get(siteID).add(select);
				}				
			}	
						
			if (node.selects.size() == 0) {
				int upper = 4;
				if (node.tableName.equals("customer"))
					upper = 1;
				if (node.tableName.equals("book"))
					upper = 3;
				for (int siteID = 1; siteID <= upper; ++siteID) {
					selectsPerSite.set(siteID, new ArrayList<SQLWhere>());
				}
			}
			
			try {
				for (int siteID = 1; siteID < selectsPerSite.size(); ++siteID) 
				if (selectsPerSite.get(siteID) != null) {	
					ArrayList<SQLWhere> selects = selectsPerSite.get(siteID);
					String selectsString = SQLWhere.UnionSelect(selects);
										
					TableMeta tempTableMeta = 
							rmiClients[siteID].select(node.tableName, selectsString);	
					
					if (tableMeta == null)
						tableMeta = tempTableMeta;
					else
						tableMeta = rmiClients[tableMeta.siteID].union(tableMeta, tempTableMeta);		

					for (SQLWhere select : selects) 
					if (select.type.equals("ED") || select.type.equals("JACCARD")) {		
						MyLogger.log("select = " + select);
						tableMeta = rmiClients[siteID].selectBySimilarity(
								tableMeta, 
								select.fullAttribute1(), 
								select.tail, 
								select.threshold,
								select.type);						
					}
				}				
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}		
		
		if (node.type.equals("join")) {
			assert(tableMeta1 != null);
			assert(tableMeta2 != null);
			int mergeSite = tableMeta1.siteID;
			if (tableMeta1.numRows > tableMeta2.numRows) {
				mergeSite = tableMeta2.siteID;
			}
			try {
				tableMeta = rmiClients[mergeSite].join(
						tableMeta1, 
						tableMeta2, 
						node.join.fullAttribute1(), 
						node.join.fullAttribute2());					
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
			System.out.println(tableMeta);
		}
		
		return tableMeta;
	}
	
	public Table execute() {		
		TableMeta tableMeta = executeNode(this.root);
		try {
			return rmiClients[ tableMeta.siteID ].getTableByID(tableMeta.idInLocalSite);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	public String toString() {
		StringBuilder content = new StringBuilder();
		for (SQLTreeNode node : nodes)
			content.append( node.toString() + "\n" );
		return content.toString();
	}

	public void clear() {
		// TODO Auto-generated method stub
		nodes.clear();
		leafTable.clear();
		root = null;
	}
}
