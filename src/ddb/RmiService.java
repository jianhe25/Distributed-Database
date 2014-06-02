package ddb;

import java.rmi.Remote;
import java.rmi.RemoteException;


public interface RmiService extends Remote {
	/**
	 * select in a certain site
	 *
	 * @param query 
	 * 				the query string
	 * 				[0] table
	 * 				[1] where clause
	 * @return
	 * 				the int that the new query result in the site
	 * @throws RemoteException
	 */
	public TableMeta select(String tableName, String whereClause) throws RemoteException;
	
	
	/**
	 * union the two results which are already in the temporary site;
	 * @param siteId1
	 * @param siteId2
	 * @return
	 * @throws RemoteException
	 */
	public TableMeta union(TableMeta tableMeta1, TableMeta tableMeta2) throws RemoteException;
	
	/**
	 * project the colomn we want
	 * 
	 * @param siteId
	 * 				the id we need to project
	 * @param columnNames
	 * 				the string including the colomn names
	 * @return
	 * @throws RemoteException
	 */
	public TableMeta project(int tableID, String[] columnNames) throws RemoteException;
	
	/**
	 * join the two table
	 * @param siteId1
	 * 				the first table
	 * @param siteId2
	 * 				the second table
	 * @param column
	 * 				table1.column = table2.column
	 * @return numOfTuple
	 * @throws RemoteException
	 */
	public TableMeta join(TableMeta tableMeta1, TableMeta tableMeta2, 
			 String keyColumn1, String keyColumn2) throws RemoteException;
	
	public Table getTableByID(int id) throws RemoteException;


	/**
	 * @example ED(table.attr, "query") < 3
	 * 			JACCARD(table.attr, "query who are you") > 0.7
	 * @param tableMeta: input table
	 * @param columnName: column searched 
	 * @param query: "query"
	 * @param threshold: edit-distance or jaccard threshold
	 * @param similarType: "ED" or "JACCARD"
	 * @return result table
	 */
	public TableMeta selectBySimilarity(TableMeta tableMeta, 
			String columnName,
			String query, 
			double threshold, 
			String similarType) throws RemoteException;

}
