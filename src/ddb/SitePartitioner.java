package ddb;

import java.util.ArrayList;



public class SitePartitioner {	
	String[] hostURLs = {"this is not used",
			"jdbc:mysql://localhost:3306/ddb1?useUnicode=true&characterEncoding=utf8",
			"jdbc:mysql://localhost:3306/ddb2?useUnicode=true&characterEncoding=utf8",
			"jdbc:mysql://localhost:3306/ddb3?useUnicode=true&characterEncoding=utf8",
			"jdbc:mysql://localhost:3306/ddb4?useUnicode=true&characterEncoding=utf8"
			};
	
	String username = "root";
	String password = "123456";
	
	public static ArrayList<Integer> calcSiteFromSelect(SQLWhere select) {
		String table = select.table1;
		String attribute = select.attribute1;
		String value = select.tail;
		ArrayList<Integer> siteIDs = new ArrayList<Integer>();
		if (table.equals("publisher")) {
			if (attribute.equals("id")) {
				int id = Integer.parseInt(value);				 
				if (id < 104000) {
					siteIDs.add(1);
					siteIDs.add(2);
				} else {
					siteIDs.add(3);
					siteIDs.add(4);
				}
			} else
			if (attribute.equals("nation")) {
				String nation = value;
				if (nation.equals("RC")) 
					siteIDs.add(1);
				if (nation.equals("USA")) {
					siteIDs.add(2);
					siteIDs.add(4);
				}
				if (nation.equals("PRC"))
					siteIDs.add(3);				
			} else {
				siteIDs.add(1);
				siteIDs.add(2);
				siteIDs.add(3);
				siteIDs.add(4);
				MyLogger.log("Non exist attribute in publisher selection");
			}
		}
		
		if (table.equals("book")) {
			if (attribute.equals("id")) {
				int id = Integer.parseInt(value);
				if (id < 205000)
					siteIDs.add(1);
				if (id >= 205000 && id < 210000)
					siteIDs.add(2);
				if (id >= 210000)
					siteIDs.add(3);			
			} else {
				siteIDs.add(1);
				siteIDs.add(2);
				siteIDs.add(3);
				MyLogger.log("Non exist attribute in book selection");
			}
		}
		
		if (table.equals("customer")) {
			if (attribute.equals("id")) {
				siteIDs.add(1);
				siteIDs.add(2);
			} else 
			if (attribute.equals("name")) {
				siteIDs.add(1);
			} else 
			if (attribute.equals("rank")) {
				siteIDs.add(2);
			} else {
				siteIDs.add(1);
				siteIDs.add(2);
				MyLogger.log("Non exist attribute in customer selection");
			}
		}
		
		if (table.equals("order")) {
			if (attribute.equals("customer_id")) {
				int customer_id = Integer.parseInt(value);
				if (customer_id < 307000) {
					siteIDs.add(1);
					siteIDs.add(2);
				} else {
					siteIDs.add(3);
					siteIDs.add(4);
				}
			} else 
			if (attribute.equals("book_id")) {
				int book_id = Integer.parseInt(value);
				if (book_id < 215000) {
					siteIDs.add(1);
					siteIDs.add(3);
				} else {
					siteIDs.add(2);
					siteIDs.add(4);
				}
			} else {				
				siteIDs.add(1);
				siteIDs.add(2);
				siteIDs.add(3);
				siteIDs.add(4);
				MyLogger.log("Non exist attribute in customer selection");
			}			
		}		
		return siteIDs; 		
	}
}
 