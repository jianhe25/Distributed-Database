package ddb.test;


import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import ddb.SQLCommand;
import ddb.SQLProcessor;
import ddb.Table;

public class SQLProcessorTest {

	SQLProcessor sqlProcessor;
	@Before
	public void setUp() throws Exception {
		sqlProcessor = new SQLProcessor();
	}

	@Test
	public void testParse() {
		String command = "select book.id, publisher.id " + 
						 "from book, publisher " + 
						 "where book.id > 200000 and " +
						 "publisher.id > 100000 and " + 
						 "book.id = publisher.id and " +
						 "book.title = \"what\";";
		
		SQLCommand sqlCommand = sqlProcessor.parse(command);
		System.out.println(sqlCommand.toString());
		assertEquals(sqlCommand.projects, 
				new ArrayList<String>(Arrays.asList("book.id", "publisher.id")));
		
		assertEquals(sqlCommand.froms,
				new ArrayList<String>(Arrays.asList("book", "publisher")));
		assertEquals(sqlCommand.joins.size(), 1);
		assertEquals(sqlCommand.selects.size(), 3);		
	}

	@Test
	public void testParseSimilarity() {
		String command = "select book.id, publisher.id " + 
						 "from book, publisher " + 
						 "where book.id > 200000 and " +
						 "publisher.id > 100000 and " + 
						 "book.id = publisher.id and " +
						 "ED(book.authors, \"whoami\") < 10 and " +
						 "JACCARD(book.title, \"who are you\") > 0.8;";
		
		SQLCommand sqlCommand = sqlProcessor.parse(command);
		System.out.println(sqlCommand.toString());
		assertEquals(sqlCommand.selects.size(), 4);
	}
	
	@Test
	public void testExecute() {
		String command = "select * " + 
						 " from book " + 
						 " where ED(book.authors, \"Amy Tan\") < 3 and " +
						 " JACCARD(book.title, \"You Can Draw a Kangaroo\") > 0.3;";
		
		Table table = sqlProcessor.execute(command);
		System.out.println("********************************\n" + table.toString());
	}
}
