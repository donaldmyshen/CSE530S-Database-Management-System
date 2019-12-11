 
package hw5;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import com.google.gson.JsonObject;

public class DBCollection {
	DB database; 
	String name;
	String dir; 
	ArrayList<JsonObject> documents; 
	
	public ArrayList<JsonObject> getAllDocuments() {
		return this.documents;
	}

	/**
	 * Constructs a collection for the given database with the given name. If that
	 * collection doesn't exist it will be created.
	 */
	public DBCollection(DB database, String name) {
		this.database = database;
		this.name = name;
		dir = database.getDir() + "/" + name + ".json";
		documents = new ArrayList<>();
		if (!new File(dir).exists()) {
			try {
				new File(dir).createNewFile();
				return;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		try {
			File file = new File(dir);
			Scanner scan = new Scanner(new FileReader(file));
			StringBuilder sb = new StringBuilder();
			while (scan.hasNextLine()) {
				String line = scan.nextLine();
				if (line.equals("\t")) {
					documents.add(Document.parse(sb.toString()));
					sb.setLength(0);
					continue;
				}
				sb.append(line + "\n");
			}
			documents.add(Document.parse(sb.toString()));
			scan.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Inserts documents into the collection Must create and set a proper id before
	 * insertion When this method is completed, the documents should be permanently
	 * stored on disk.
	 * 
	 * @param documents
	 */
	public void insert(JsonObject... documents) {
		for (JsonObject document : documents) {
			String id = this.name + ":" + this.documents.size();
			document.addProperty("_id", id);
			this.documents.add(document);
		}
		writeIntoDisk();
	}

	/**
	 * Locates one or more documents and replaces them with the update document.
	 * 
	 * @param query  relational select for documents to be updated
	 * @param update the document to be used for the update
	 * @param multi  true if all matching documents should be updated false if only
	 *               the first matching document should be updated
	 */
	public void update(JsonObject query, JsonObject update, boolean multi) {
		DBCursor cursor = this.find(query);
		while (cursor.hasNext()) {
			JsonObject document = cursor.next();
			int index = this.documents.indexOf(document);
			this.documents.remove(index);
			// use index to add to certain place
			this.documents.add(index, update);
			if (!multi)
				break;
		}
		writeIntoDisk();
	}

	/**
	 * Removes one or more documents that match the given query parameters
	 * 
	 * @param query relational select for documents to be removed
	 * @param multi true if all matching documents should be updated false if only
	 *              the first matching document should be updated
	 */
	public void remove(JsonObject query, boolean multi) {
		DBCursor cursor = this.find(query);
		while (cursor.hasNext()) {
			JsonObject document = cursor.next();
			this.documents.remove(document);
			if (!multi)
				break;
		}
		writeIntoDisk();
	}

	private void writeIntoDisk() {
		File file = new File(this.dir);
		FileOutputStream output;
		try {
			StringBuilder sb = new StringBuilder();
			for (JsonObject obj : documents)
				sb.append(Document.toJsonString(obj) + "\n\t\n");
			sb.append(Document.toJsonString(this.documents.get(documents.size() - 1)));
			output = new FileOutputStream(file, false);
			output.write(sb.toString().getBytes());
			output.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	/**
	 * Returns a cursor for all of the documents in this collection.
	 */
	public DBCursor find() {
		return new DBCursor(this, null, null);
	}

	/**
	 * Finds documents that match the given query parameters.
	 * 
	 * @param query relational select
	 * @return
	 */
	public DBCursor find(JsonObject query) {
		return new DBCursor(this, query, null);
	}

	/**
	 * Finds documents that match the given query parameters.
	 * 
	 * @param query      relational select
	 * @param projection relational project
	 * @return
	 */
	public DBCursor find(JsonObject query, JsonObject projection) {
		return new DBCursor(this, query, projection);
	}

	/**
	 * Returns the ith document in the collection. Documents are separated by a line
	 * that contains only a single tab (\t) Use the parse function from the document
	 * class to create the document object
	 */
	public JsonObject getDocument(int i) {
		return this.documents.get(i);
	}

	/**
	 * Drops this collection, removing all of the documents it contains from the DB
	 */
	public void drop() {
		File file = new File(this.dir);
		file.delete();
		this.documents.clear();
	}
	
	/**
	 * Returns the number of documents in this collection
	 */
	public long count() {
		return this.documents.size();
	}

	public String getName() {
		return this.name;
	}

}
