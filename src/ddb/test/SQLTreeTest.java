package ddb.test;



import org.junit.Before;
import org.junit.Test;

import ddb.MyLogger;
import ddb.SQLProcessor;
import ddb.SQLTree;
import ddb.Table;

public class SQLTreeTest {

	SQLTree sqlTree;
	SQLProcessor sqlProcessor;
	@Before
	public void setUp() throws Exception {	
		sqlProcessor = new SQLProcessor();		
	}

//	@Test
//	public void testBuildTree() {		
//		sqlTree = new SQLTree();
//		String command = "select book.id, publisher.id " + 
//				 "from book, publisher " + 
//				 "where book.id > 200000 and " +
//				 "publisher.id > 100000 and " + 
//				 "book.id = publisher.id and " +
//				 "book.name = 'what' ";
//		SQLCommand sqlCommand = sqlProcessor.parse(command);
//		try {
//			sqlTree.buildTree(sqlCommand);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		System.out.println("sqlTree #1");
//		System.out.println(sqlTree.toString());
//		
//		sqlTree = new SQLTree();
//		command = "select * " +
//				  "from customer;";
//		try {
//			sqlTree.buildTree(sqlProcessor.parse(command));
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		System.out.println("sqlTree #2");
//		System.out.println(sqlTree.toString());
//	}
	
//	@Test
//	public void testExecuteTree() {			
//		// Test select
//		sqlTree = new SQLTree();
//		String command = "select * from customer;";
//		
//		try {
//			sqlTree.buildTree(sqlProcessor.parse(command));
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		System.out.println(sqlTree.toString());
//		Table result = sqlTree.execute();
//		System.out.println("********************\n" + result.toString());
//		
//		// Test project
//		sqlTree = new SQLTree();
//		command = "select customer.name from customer;";
//		try {
//			sqlTree.buildTree(sqlProcessor.parse(command));
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		result = sqlTree.execute();
//		System.out.println("********************\n" + result.toString());
//		
//		
//		sqlTree.clear();
//		command = "select publisher.name from publisher where publisher.id < 120000;";
//		try {
//			sqlTree.buildTree(sqlProcessor.parse(command));
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		result = sqlTree.execute();
//		System.out.println("********************\n" + result.toString());
//	}
//	
//	@Test
//	public void testJoin() {	
//		sqlTree = new SQLTree();
//		String command = "select book.title, orders.book_id, orders.customer_id" +
//				  " from book, orders " +
//				  " where book.id = orders.book_id;";
//		try {
//			sqlTree.buildTree(sqlProcessor.parse(command));
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		System.out.println(sqlTree.toString());
//		Table result = sqlTree.execute();
//		System.out.println("********************\n" + result.toString());		
//	}
	
	@Test
	public void testSimilaritySearch() {	
		sqlTree = new SQLTree();
		String command = "select book.title, book.authors" +
				  " from book " +
				  " where ED(book.authors, \"Amy Tan\") < 3;";
		try {
			sqlTree.buildTree(sqlProcessor.parse(command));
		} catch (Exception e) {
			e.printStackTrace();
		}
		MyLogger.log(sqlTree.toString());
		Table result = sqlTree.execute();
		System.out.println("********************\n" + result.toString());	
		
		sqlTree.clear();
		command = "select book.title, book.authors" +
				  " from book " +
				  " where JACCARD(book.title, \"Above the Thunder: A Novel\") > 0.2;";
		try {
			sqlTree.buildTree(sqlProcessor.parse(command));
		} catch (Exception e) {
			e.printStackTrace();
		}
		MyLogger.log(sqlTree.toString());
		result = sqlTree.execute();
		System.out.println("********************\n" + result.toString());
	}
}
