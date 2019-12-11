 
package test;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;

import hw5.DB;
import hw5.DBCollection;
import hw5.Document;

class DocumentTester {
	
	/*
	 * Things to consider testing:
	 * 
	 * Invalid JSON
	 * 
	 * Properly parses embedded documents
	 * Properly parses arrays
	 * Properly parses primitives (done!)
	 * 
	 * Object to embedded document
	 * Object to array
	 * Object to primitive
	 */
	
	@Test
	public void testParsePrimitive() {
		String json = "{ \"key\":\"value\" }";//setup
		JsonObject results = Document.parse(json); //call method to be tested
		assertTrue(results.getAsJsonPrimitive("key").getAsString().equals("value")); //verify results
	}
	
	@Test
	public void toJsonString() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		JsonObject json = test.getDocument(1);
		String json_str = Document.toJsonString(json);
		JsonObject results = Document.parse(json_str);
		assertTrue(results.getAsJsonObject("embedded").getAsJsonPrimitive("key2").getAsString().equals("value2"));
	}
	
	@Test
	public void toJsonString1() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		JsonObject json = test.getDocument(1);
		String json_str = Document.toJsonString(json);
		JsonObject results = Document.parse(json_str);
		assertTrue(results.getAsJsonObject("info").getAsJsonPrimitive("month").getAsString().equals("11"));
	}
	
	@Test
	public void toJsonString2() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		JsonObject json = test.getDocument(1);
		String json_str = Document.toJsonString(json);
		JsonObject results = Document.parse(json_str);
		assertTrue(results.getAsJsonObject("info").getAsJsonPrimitive("year").getAsString().equals("2018"));
	}
	
	@Test
	public void toJsonString3() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		JsonObject json = test.getDocument(3);
		String json_str = Document.toJsonString(json);
		JsonObject results = Document.parse(json_str);
		assertTrue(results.getAsJsonObject("info").getAsJsonPrimitive("condition").getAsString().equals("new"));
	}
	
	@Test
	public void toJsonString4() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		JsonObject json = test.getDocument(0);
		String json_str = Document.toJsonString(json);
		JsonObject results = Document.parse(json_str);
		assertTrue(results.getAsJsonObject("info").getAsJsonPrimitive("month").getAsString().equals("02"));
	}

}
