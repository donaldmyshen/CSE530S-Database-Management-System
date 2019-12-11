 
package hw5;

import java.io.File;
import java.util.HashMap;

public class DB {

	private HashMap<String, DBCollection> collections;
	private String dir;
	private File root;

	// Get the directory of this database
	public String getDir() {
		return dir;
	}

	/**
	 * Creates a database object with the given name. The name of the database will
	 * be used to locate where the collections for that database are stored. For
	 * example if my database is called "library", I would expect all collections
	 * for that database to be in a directory called "library".
	 * 
	 * If the given database does not exist, it should be created.
	 */
	public DB(String name) {
		dir = "testfiles/" + name;
		collections = new HashMap<>();
		root = new File("testfiles/" + name);
		boolean exist = root.exists();
		if (!exist) {
			new File("testfiles/" + name).mkdirs();
			return;
		}
		for (String file : root.list()) {
			DBCollection collection = new DBCollection(this, file.split("\\.")[0]);
			collections.put(file.split("\\.")[0], collection);
		}
	}

	/**
	 * Retrieves the collection with the given name from this database. The
	 * collection should be in a single file in the directory for this database.
	 * 
	 * Note that it is not necessary to read any data from disk at this time. Those
	 * methods are in DBCollection.
	 */
	public DBCollection getCollection(String name) {
		return collections.getOrDefault(name, new DBCollection(this, name));
	}

	/**
	 * Drops this database and all collections that it contains
	 */
	public void dropDatabase() {
		File root = new File(this.dir);
		for (String name : root.list()) {
			File file = new File(root.getPath(), name);
			file.delete();
		}
		root.delete();
		collections.clear();
	}
}
