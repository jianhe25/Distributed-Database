package ddb.test;

import static org.junit.Assert.*;

import java.rmi.RemoteException;

import org.junit.Before;
import org.junit.Test;

import ddb.RmiServer;
import ddb.RmiService;
import ddb.TableMeta;

public class RmiServerTest {

	RmiService[] rmiClients = new RmiServer[5];
	@Before
	public void setUpBeforeClass() throws Exception {
		// TODO Auto-generated method stub
		try {			
			RmiServer.runServers();
			rmiClients = RmiServer.getClients();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
//	@After
//	public void tearDownAfterClass() throws Exception {
//		try {			
//			RmiServer.stopServers();		
//		} catch (RemoteException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}	
//	}
	
	@Test
	public void testSelect() {
		try {
			// Test empty whereClause
			TableMeta tableMeta = rmiClients[1].select("book", " ");
			assertEquals(tableMeta.numRows, 4999);
			
			
			// Test operator > 
			tableMeta = rmiClients[1].select("book", " where book.id > 202000");
			assertEquals(tableMeta.numRows, 2999);			
						
			// Test operator = 
			tableMeta = rmiClients[1].select("book", " where book.id = 202000");
			assertEquals(tableMeta.numRows, 1);	
			
			// Test server 2
			tableMeta = rmiClients[2].select("publisher", "");
			assertEquals(tableMeta.numRows, 2005);			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

	@Test
	public void testProject() {
		try {
			// Test empty whereClause
			TableMeta tableMeta = rmiClients[1].select("book", " LIMIT 10 ");
			assertEquals(tableMeta.numRows, 10);
			
			TableMeta resultMeta = rmiClients[1].project(tableMeta.idInLocalSite, new String[]{"book.title", "book.id", "costumer.id"});
			assertEquals(resultMeta.columnNames.get(0), "book.id");
			assertEquals(resultMeta.columnNames.get(1), "book.title");
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testUnion() {
		try {
			// Test empty whereClause
			TableMeta tableMeta1 = rmiClients[1].select("book", " LIMIT 10 ");
			assertEquals(tableMeta1.numRows, 10);
			TableMeta tableMeta2 = rmiClients[2].select("book", " LIMIT 100");
			assertEquals(tableMeta2.numRows, 100);
		
			 
			TableMeta tableMeta3 = rmiClients[1].union(tableMeta1, tableMeta2);
			
			assertEquals(tableMeta3.numRows, 110);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testJoin() {
		try {
			// Test empty whereClause
			TableMeta tableMeta1 = rmiClients[1].select("publisher", "");
			TableMeta tableMeta2 = rmiClients[1].select("book", "");											
			TableMeta tableMeta3 = rmiClients[1].join(tableMeta1, tableMeta2, "publisher.id", "book.publisher_id");
			assertEquals(tableMeta3.numRows, 2026);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
