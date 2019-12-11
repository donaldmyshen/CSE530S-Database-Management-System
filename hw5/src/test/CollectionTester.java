 
package test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;

import hw5.DB;
import hw5.DBCollection;
import hw5.DBCursor;
import hw5.Document;

class CollectionTester {
	
	/**
	 * Things to consider testing
	 * 
	 * Queries:
	 * 	Find all
	 * 	Find with relational select
	 * 		Conditional operators
	 * 		Embedded documents
	 * 		Arrays
	 * 	Find with relational project
	 * 
	 * Inserts
	 * Updates
	 * Deletes
	 * 
	 * getDocument (done?)
	 * drop
	 */
	
	@Test
	public void testGetDocument() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		JsonObject primitive = test.getDocument(0);
		assertTrue(primitive.getAsJsonPrimitive("key").getAsString().equals("value"));
	}
	
	@Test
	public void testGetCollectionNoExist() {
		assertTrue(new File("testfiles/data/res1.json").exists());
	}

	@Test
	public void testDocumentInsertion() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		ArrayList<JsonObject> documents = test.getAllDocuments();
		JsonObject[] documents_array = new JsonObject[documents.size()];
		DBCollection test2 = db.getCollection("res2");
		long size_0 = test2.count();
		test2.insert(documents.toArray(documents_array));
		assertTrue(test2.count() - size_0 == 3);
	}

	@Test
	public void testDocumentInsertion1() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist"); 
		ArrayList<JsonObject> documents = test.getAllDocuments();
		JsonObject[] documents_array = new JsonObject[documents.size()];
		DBCollection test3 = db.getCollection("res3");
		long size_0 = test3.count();
		test3.insert(documents.toArray(documents_array));
		assertTrue(test3.count() - size_0 == 5);
	}

	// Res4 is cleared every time this test runs
	@Test
	public void testDocumentRemove() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist"); // mytest.json is in testfiles/data/
		ArrayList<JsonObject> documents = test.getAllDocuments();
		JsonObject[] documents_array = new JsonObject[documents.size()];
		DBCollection test4 = db.getCollection("res4");
		test4.drop();
		test4 = db.getCollection("res4");
		long size_0 = test4.count();
		test4.insert(documents.toArray(documents_array));
		assertTrue(test4.count() - size_0 == 5);
	}
	
	@Test
	public void testDocumentRemove1() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		ArrayList<JsonObject> documents = test.getAllDocuments();
		JsonObject[] documents_array = new JsonObject[documents.size()];
		DBCollection test4 = db.getCollection("res4");
		test4.drop();
		test4 = db.getCollection("res4");
		test4.insert(documents.toArray(documents_array));
		long size_1 = test4.count();
		test4.remove(Document.parse("{\"info\":{ \"year\": \"2019\", \"month\": \"03\", \"condition\": \"new\"}}"),
				true);
		// Remove one
		assertTrue(size_1 - test4.count() == 1);
	}
	
	@Test
	public void testDocumentRemove2() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		ArrayList<JsonObject> documents = test.getAllDocuments();
		JsonObject[] documents_array = new JsonObject[documents.size()];
		DBCollection test4 = db.getCollection("res4");
		test4.drop();
		test4 = db.getCollection("res4");
		test4.insert(documents.toArray(documents_array));
		long size_1 = test4.count();
		test4.remove(Document.parse("{\"info\":{ \"year\": \"2018\", \"month\": \"11\", \"condition\": \"used\" }}"),
				true);
		// Remove two
		assertTrue(size_1 - test4.count() == 2);
	}
	
	@Test
	public void testDocumentRemove3() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist"); // mytest.json is in testfiles/data/
		ArrayList<JsonObject> documents = test.getAllDocuments();
		JsonObject[] documents_array = new JsonObject[documents.size()];
		DBCollection test4 = db.getCollection("res4");
		test4.drop();
		test4 = db.getCollection("res4");
		test4.insert(documents.toArray(documents_array));
		long size_1 = test4.count();
		test4.remove(Document.parse("{\"info\":{ \"year\": \"2010\", \"condition\": \"new\" }}"),
				true);
		//Remove 0
		assertTrue(size_1 - test4.count() == 0);
	}

	@Test
	public void testDocumentUpdate() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		ArrayList<JsonObject> documents = test.getAllDocuments();
		JsonObject[] documents_array = new JsonObject[documents.size()];
		DBCollection test5 = db.getCollection("res5");
		test5.getAllDocuments().clear();
		test5.insert(documents.toArray(documents_array));
		test5.update(Document.parse("{\"info.year\":{\"$in\":[\"2018\",\"2014\",\"2005\"]}, \"make\":\"bmw\"}"),
				Document.parse("{\"update_key\": \"value\"}"), true);
		DBCursor results = test5.find(Document.parse("{\"update_key\": \"value\"}"));
		assertTrue(results.count() == 1);
	}
	
	@Test
	public void testDocumentUpdate1() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		ArrayList<JsonObject> documents = test.getAllDocuments();
		JsonObject[] documents_array = new JsonObject[documents.size()];
		DBCollection test5 = db.getCollection("res5");
		test5.getAllDocuments().clear();
		test5.insert(documents.toArray(documents_array));
		test5.update(Document.parse("{\"info.year\":{\"$in\":[\"2018\",\"2005\"]}}"),
				Document.parse("{\"update_key\": \"value\"}"), true);
		DBCursor results = test5
				.find(Document.parse("{\"update_key\": \"value\"}"));
		assertTrue(results.count() == 2);
	}
	
	@Test
	public void testDocumentUpdate2() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		ArrayList<JsonObject> documents = test.getAllDocuments();
		JsonObject[] documents_array = new JsonObject[documents.size()];
		DBCollection test5 = db.getCollection("res5");
		test5.getAllDocuments().clear();
		test5.insert(documents.toArray(documents_array));
		test5.update(Document.parse("{\"info.condition\":{\"$in\":[\"new\"]}}"),
				Document.parse("{\"update_key\": \"value\"}"), true);
		DBCursor results = test5.find(Document.parse("{\"update_key\": \"value\"}"));
		assertTrue(results.count() == 3);
	}
	
	@Test
	public void testDocumentUpdate3() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("carlist");
		ArrayList<JsonObject> documents = test.getAllDocuments();
		JsonObject[] documents_array = new JsonObject[documents.size()];
		DBCollection test5 = db.getCollection("res5");
		test5.getAllDocuments().clear();
		test5.insert(documents.toArray(documents_array));
		test5.update(Document.parse("{\"info.month\":{\"$in\":[\"09\", \"11\"]}}"),
				Document.parse("{\"update_key\": \"value\"}"), true);
		DBCursor results = test5.find(Document.parse("{\"update_key\": \"value\"}"));
		assertTrue(results.count() == 2);
	}
}
