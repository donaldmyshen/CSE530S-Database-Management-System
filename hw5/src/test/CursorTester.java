 
package test;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;

import hw5.DB;
import hw5.DBCollection;
import hw5.DBCursor;
import hw5.Document;

class CursorTester {

	/**
	 * Things to consider testing:
	 * 
	 * hasNext (done?) count (done?) next (done?)
	 */

	@Test
	public void testFindAll() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find();

		assertTrue(results.count() == 3);
		assertTrue(results.hasNext());
		JsonObject d1 = results.next(); // pull first document
		// verify contents?
		assertTrue(results.hasNext());// still more documents
		JsonObject d2 = results.next(); // pull second document
		// verfiy contents?
		assertTrue(results.hasNext()); // still one more document
		JsonObject d3 = results.next();// pull last document
		assertFalse(results.hasNext());// no more documents
	}

	// Test: Queries that request a single document from the collection
	@Test
	public void testQuery1() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\"key\":\"value\"}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(results.next().get("key").getAsJsonPrimitive().getAsString().equals("value"));
	}

	// Test: Queries based on data in an embedded document
	@Test
	public void testQuery2_embedded() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\"embedded.key2\":\"value2\"}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(results.next().get("embedded").getAsJsonObject().get("key2").getAsJsonPrimitive().getAsString()
				.equals("value2"));
	}

	// Test for <key: JsonObject> query
	@Test
	public void testQuery3_jsonobject() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\"embedded\":{\"key2\":\"value2\"}}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(results.next().get("embedded").getAsJsonObject().get("key2").getAsJsonPrimitive().getAsString()
				.equals("value2"));
	}

	// Test: Queries for <key, array>
	@Test
	public void testQuery4_jsonarray() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\r\n" + "	\"array\": [ \"one\", \"two\", \"three\" ]\r\n" + "}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(
				results.next().get("array").getAsJsonArray().get(0).getAsJsonPrimitive().getAsString().equals("one"));
	}

	// Test: Queries for array
	@Test
	public void testQuery5() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\"array\":\"two\"}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(
				results.next().get("array").getAsJsonArray().get(0).getAsJsonPrimitive().getAsString().equals("one"));
	}

	// Test $gt
	@Test
	public void testQuery6() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\"key\":{$gt:\"valu\"}}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(results.next().get("key").getAsJsonPrimitive().getAsString().equals("value"));
	}

	// Test $lt
	@Test
	public void testQuery7() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\"key\":{$lt:\"value1\"}}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(results.next().get("key").getAsJsonPrimitive().getAsString().equals("value"));
	}

	// Test $in: value in document is JsonPrimitive
	@Test
	public void testQuery8() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\"key\":{$in:[\"value1\",\"value\"]}}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(results.next().get("key").getAsJsonPrimitive().getAsString().equals("value"));
	}

	// Test $in: value in document is JsonArray
	@Test
	public void testQuery9() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find(Document.parse("{\"array\":{$in:[\"one\",\"value\"]}}"));
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(
				results.next().get("array").getAsJsonArray().get(0).getAsJsonPrimitive().getAsString().equals("one"));
	}

	//Tests based on carlist.JSON
	@Test
	public void testEmptyQuery() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		DBCursor results = test.find(Document.parse("{}"));
		assertTrue(results.count() == 5);
	}
	
	@Test
	public void testQueryCarlist() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		DBCursor results = test.find(Document.parse("{\"info\":{ \"year\": \"2014\", \"month\": \"02\", \"condition\": \"new\" }}"));
		assertTrue(results.count() == 1);
		System.out.println(results.next().toString());
	}
	
	@Test
	public void testQueryCarlist1() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		DBCursor results = test.find(Document.parse("{\"info\":{ \"year\": \"2018\", \"month\": \"11\", \"condition\": \"used\" }}"));
		assertTrue(results.count() == 2);
		System.out.println(results.next().toString());
	}
	
	@Test
	public void testQueryCarlist2() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		DBCursor results = test.find(Document.parse("{ \"info.year\": { \"$ne\": \"2015\" }, \"info.condition\": \"used\","
						+ " \"rate\": \"B+\" }"));
		assertTrue(results.count() == 1);
		System.out.println(results.next().toString());
	}
	
	@Test
	public void testQueryCarlist3() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		DBCursor results = test.find(Document.parse("{ \"info.month\": { \"$gte\": \"05\" }, \"info.condition\": \"used\","
						+ " \"rate\": \"B+\" }"));
		assertTrue(results.count() == 1);
		System.out.println(results.next().toString());
	}
	
	@Test
	public void testQueryCarlist4() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		DBCursor results = test.find(Document.parse("{\"info.condition\": \"used\"," + " \"rate\": \"B+\" }"));
		assertTrue(results.count() == 1);
		System.out.println(results.next().toString());
	}



	@Test
	public void testProjection() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		DBCursor results = test.find(Document.parse("{}"), Document.parse("{ \"make\": \"1\", \"rate\": \"1\" }"));
		assertTrue(results.count() == 5);
		while (results.hasNext()) {
			assertTrue(results.next().has("make") == true);
		}
	}

	@Test
	public void testProjection1() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		DBCursor results = test.find(Document.parse("{}"), Document.parse("{ \"make\": 0, \"rate\": 0 }"));
		assertTrue(results.count() == 5);
		while (results.hasNext()) {
			assertTrue(results.next().has("make") == false);
		}
	}
	
	@Test
	public void testEmbeddedProjection() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		DBCursor results = test.find(Document.parse("{}"),
				Document.parse("{ \"make\": 1, \"info.year\": 1, \"info.month\": 1 }"));
		assertTrue(results.count() == 5);
		while (results.hasNext()) {
			System.out.println(results.next().toString());
		}
	}
	
	// Tests based on carstock.JSON
	@Test
	public void testQueryStock() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carstock");
		DBCursor results = test.find(Document.parse("{ \"instock.0.warehouse\": \"A\"}"));
		assertTrue(results.count() == 3);
	}
	
	@Test
	public void testQueryStock1() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carstock");
		DBCursor results = test.find(Document.parse("{ \"instock.0.warehouse\": \"B\"}"));
		assertTrue(results.count() == 1);
	}
	
	@Test
	public void testQueryStock2() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carstock");
		DBCursor results = test.find(Document.parse("{ \"instock.0.warehouse\": \"C\"}"));
		assertTrue(results.count() == 1);
	}
	
	@Test
	public void testQueryStock3() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carstock");
		DBCursor results = test.find(Document.parse("{ \"instock.0.qty\": {\"$lte\": 30}}"));
		System.out.println(results.count());
		assertTrue(results.count() == 4);
		while (results.hasNext()) {
			System.out.println(results.next().toString());
		}
	}
	
	@Test
	public void testQueryStock4() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carstock");
		DBCursor results = test.find(Document.parse("{ \"instock.0.qty\": {\"$lt\": 30}}"));
		System.out.println(results.count());
		assertTrue(results.count() == 3);
		while (results.hasNext()) {
			System.out.println(results.next().toString());
		}
	}
	
	@Test
	public void testQueryStock5() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carstock");
		DBCursor results = test.find(Document.parse("{ \"instock.0.qty\": {\"$gte\": 20}}"));
		System.out.println(results.count());
		assertTrue(results.count() == 4);
		while (results.hasNext()) {
			System.out.println(results.next().toString());
		}
	}
	
	@Test
	public void testQueryStock6() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carstock");
		DBCursor results = test.find(Document.parse("{ \"instock.0.qty\": {\"$gt\": 20}}"));
		System.out.println(results.count());
		assertTrue(results.count() == 2);
		while (results.hasNext()) {
			System.out.println(results.next().toString());
		}
	}
	

}
