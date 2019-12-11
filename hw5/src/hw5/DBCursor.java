 
package hw5;

import java.util.ArrayList;
import java.util.Iterator;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class DBCursor implements Iterator<JsonObject> {

	private ArrayList<JsonObject> documents;
	private int cursor; 

	public DBCursor(DBCollection collection, JsonObject query, JsonObject fields) {
		if (query == null && fields == null)
			this.documents = collection.getAllDocuments();

		if (query != null) {
			ArrayList<JsonObject> queryRes = query(collection.getAllDocuments(), query);
			this.documents = fields == null ? queryRes : project(queryRes, fields);
		}
	}

	private ArrayList<JsonObject> query(ArrayList<JsonObject> documents, JsonObject query) {
		if (query.size() == 0)
			return documents;

		ArrayList<JsonObject> res = new ArrayList<>();
		
		for (JsonObject doc : documents) {
			if (validFile(doc, query)) {
				res.add(doc);
			}
		}
		return res;
	}

	// if the document is query needs
	private boolean validFile(JsonObject document, JsonObject query) {
		for (String key : query.keySet()) {
			String[] keyAfterSplit = key.split("\\.");

			if (keyAfterSplit.length < 1)
				return false;
			
			if (keyAfterSplit.length == 1) {
				if (!document.has(key) || !findInDoc(document.get(key), query.get(key)))
					return false;
			}

			JsonElement doc = document;

			for (int i = 0; i < keyAfterSplit.length - 1; i++) {
				String s = keyAfterSplit[i];
				JsonElement element;

				if (doc.isJsonObject() && doc.getAsJsonObject().has(s)) {
					element = doc.getAsJsonObject().get(s);
					if (!element.isJsonObject() && !element.isJsonArray())
						return false;
					doc = element; // go deap
					continue;
				}

				if (doc.isJsonArray() && isInteger(s)) {
					int index = Integer.parseInt(s);
					int size = doc.getAsJsonArray().size();
					if (index < 0 || index > size)
						return false;
					element = doc.getAsJsonArray().get(index);
					if (!element.isJsonArray() && !element.isJsonObject())
						return false;
					doc = element;
					continue;
				}
				return false;
			}

			// deal the last key
			if (doc.isJsonObject() && doc.getAsJsonObject().has(keyAfterSplit[keyAfterSplit.length - 1])) {
				if (!findInDoc(doc.getAsJsonObject().get(keyAfterSplit[keyAfterSplit.length - 1]), query.get(key))) {
					return false;
				}
			}

			if (doc.isJsonArray() && isInteger(keyAfterSplit[keyAfterSplit.length - 1])) {
				int index = Integer.parseInt(keyAfterSplit[keyAfterSplit.length - 1]);
				int size = doc.getAsJsonArray().size();
				if (index >= 0 && index < size) {
					JsonElement element = doc.getAsJsonArray().get(index);
					if (!element.isJsonArray() && !element.isJsonObject())
						return false;
				}
			}
		}
		return true;
	}

	private boolean findInDoc(JsonElement doc, JsonElement query) {
		if (doc.isJsonPrimitive()) {
			if (query.isJsonPrimitive() && (doc.getAsJsonPrimitive().equals(query.getAsJsonPrimitive())))
				return true;
			if (query.isJsonObject()) {
				for (String operator : query.getAsJsonObject().keySet()) {
					// operator value is not JsonPrimitive
					if (((operator.equals("$eq") || operator.equals("$gt") || operator.equals("$gte")
							|| operator.equals("$lt") || operator.equals("$lte") || operator.equals("$ne"))
							&& query.getAsJsonObject().get(operator).isJsonPrimitive())
							|| (operator.equals("$in")
									|| operator.equals("$nin") && query.getAsJsonObject().get(operator).isJsonArray()))
						return checkOperator(operator, doc, query.getAsJsonObject().get(operator));
				}
			}
			if (query.isJsonArray())
				return false;
		}

		if (doc.isJsonObject()) {
			if (query.isJsonPrimitive() || query.isJsonArray()) {
				return false;
			}
			if (query.isJsonObject()) {
				if (doc.getAsJsonObject().toString().equals(query.getAsJsonObject().toString()))
					return true;
				for (String operator : query.getAsJsonObject().keySet()) {
					// operator value is not JsonPrimitive
					return (operator.equals("$in") || operator.equals("$nin"))
							&& query.getAsJsonObject().get(operator).isJsonArray()
							&& checkOperator(operator, doc, query.getAsJsonObject().get(operator));
				}
			}
		}

		if (doc.isJsonArray()) {
			if (query.isJsonPrimitive())
				return doc.getAsJsonArray().contains(query.getAsJsonPrimitive());
			if (query.isJsonObject()) {
				if (doc.getAsJsonArray().contains(query.getAsJsonObject()))
					return true;
				for (String operator : query.getAsJsonObject().keySet()) {
					if ((operator.equals("$eq") || operator.equals("$gt") || operator.equals("$gte")
							|| operator.equals("$lt") || operator.equals("$lte") || operator.equals("$ne"))
							&& query.getAsJsonObject().get(operator).isJsonPrimitive()) {
						for (JsonElement d : doc.getAsJsonArray()) {
							return d.isJsonPrimitive()
									&& checkOperator(operator, d, query.getAsJsonObject().get(operator));
						}
					} else if ((operator.equals("$in") || operator.equals("$nin"))
							&& query.getAsJsonObject().get(operator).isJsonArray()) {
						return checkOperator(operator, doc, query.getAsJsonObject().get(operator));
					} else {
						return false;
					}
				}
			}
			if (query.isJsonArray()) {
				return doc.getAsJsonArray().toString().equals(query.getAsJsonArray().toString());
			}
		}
		return false;
	}

	private boolean checkOperator(String operator, JsonElement docVal, JsonElement queryVal) {
		switch (operator) {
		case "$eq":
			return docVal.getAsJsonPrimitive().getAsString()
					.compareTo(queryVal.getAsJsonPrimitive().getAsString()) == 0;
		case "$gt":
			return docVal.getAsJsonPrimitive().isNumber() && queryVal.getAsJsonPrimitive().isNumber()
					? docVal.getAsJsonPrimitive().getAsDouble() > queryVal.getAsJsonPrimitive().getAsDouble()
					: docVal.getAsJsonPrimitive().getAsString()
							.compareTo(queryVal.getAsJsonPrimitive().getAsString()) > 0;
		case "$gte":
			return docVal.getAsJsonPrimitive().isNumber() && queryVal.getAsJsonPrimitive().isNumber()
					? docVal.getAsJsonPrimitive().getAsDouble() >= queryVal.getAsJsonPrimitive().getAsDouble()
					: docVal.getAsJsonPrimitive().getAsString()
							.compareTo(queryVal.getAsJsonPrimitive().getAsString()) >= 0;
		case "$lt":
			return docVal.getAsJsonPrimitive().isNumber() && queryVal.getAsJsonPrimitive().isNumber()
					? docVal.getAsJsonPrimitive().getAsDouble() < queryVal.getAsJsonPrimitive().getAsDouble()
					: docVal.getAsJsonPrimitive().getAsString()
							.compareTo(queryVal.getAsJsonPrimitive().getAsString()) < 0;
		case "$lte":
			return docVal.getAsJsonPrimitive().isNumber() && queryVal.getAsJsonPrimitive().isNumber()
					? docVal.getAsJsonPrimitive().getAsDouble() <= queryVal.getAsJsonPrimitive().getAsDouble()
					: docVal.getAsJsonPrimitive().getAsString()
							.compareTo(queryVal.getAsJsonPrimitive().getAsString()) <= 0;
		case "$ne":
			return docVal.getAsJsonPrimitive().isNumber() && queryVal.getAsJsonPrimitive().isNumber()
					? docVal.getAsJsonPrimitive().getAsDouble() != queryVal.getAsJsonPrimitive().getAsDouble()
					: docVal.getAsJsonPrimitive().getAsString()
							.compareTo(queryVal.getAsJsonPrimitive().getAsString()) != 0;
		case "$in":
			if (docVal.isJsonPrimitive()) {
				return queryVal.getAsJsonArray().contains(docVal.getAsJsonPrimitive());
			}

			if (docVal.isJsonObject()) {
				return queryVal.getAsJsonArray().contains(docVal.getAsJsonObject());
			}

			if (docVal.isJsonArray()) {
				for (JsonElement d : docVal.getAsJsonArray()) {
					if (queryVal.getAsJsonArray().contains(d)) {
						return true;
					}
				}
				return false;
			}
		case "$nin":
			if (docVal.isJsonPrimitive()) {
				return !queryVal.getAsJsonArray().contains(docVal.getAsJsonPrimitive());
			}

			if (docVal.isJsonObject()) {
				return !queryVal.getAsJsonArray().contains(docVal.getAsJsonObject());
			}

			if (docVal.isJsonArray()) {
				for (JsonElement d : docVal.getAsJsonArray()) {
					if (queryVal.getAsJsonArray().contains(d)) {
						return false;
					}
				}
				return true;
			}
		default:
			return false;
		}

	}

	private ArrayList<JsonObject> project(ArrayList<JsonObject> documents, JsonObject fields) {
		ArrayList<JsonObject> res = new ArrayList<>();
		for (JsonObject doc : documents) {

			JsonObject result = new JsonObject(); 
			for (String key : fields.keySet()) {
				JsonPrimitive value = fields.get(key).getAsJsonPrimitive();
				// exclusion
				if ((value.isString() && value.getAsString().equals("0"))
						|| (value.isNumber() && value.getAsNumber().doubleValue() == 0)) {
					result = doc;
					for (String fk : fields.keySet()) {
						JsonPrimitive fv = fields.get(fk).getAsJsonPrimitive();
						if ((fv.isString() && fv.getAsString().equals("1"))
								|| (fv.isNumber() && fv.getAsNumber().doubleValue() == 1)) {
							if (fk.equals("_id")) {
								continue;
							}
						}

						String[] arr = fk.split("\\.");

						if (arr.length == 1 && doc.has(arr[0]))
							result.remove(arr[0]);
					}
					break;
				}
			}
			// some how stupid here 
			for (String fk : fields.keySet()) {
				JsonPrimitive fv = fields.get(fk).getAsJsonPrimitive();
				if ((fv.isString() && fv.getAsString().equals("0"))
						|| (fv.isNumber() && fv.getAsNumber().doubleValue() == 0)) {
					if (fk.equals("_id")) {
						continue;
					}
				}

				String[] arr = fk.split("\\.");
				if (arr.length == 1 && doc.has(arr[0])) 
					result.add(arr[0], doc.get(arr[0]));
				else {
					JsonObject obj = findField(doc, arr);
					if (obj == null) {
						continue;
					}
					String key = arr[0];
					JsonElement value = obj.get(arr[0]);
					int j = 0;
					while (result.has(key)) {
						result = (JsonObject) result.get(key);
						j++;
						key = arr[j];
						value = ((JsonObject) value).get(key);
					}
					result.add(key, value);
				}
			}

			if (result != null) { 
				res.add(result);
			}
		}
		return res;
	}

	// find field in doc
	private JsonObject findField(JsonObject document, String[] keyAfterSplit) {
		JsonElement value = null;

		JsonElement curDoc = document;
		// record json type in level, mark json object as 0, json array as 1, or we can use boolean here,
		// but i am thinking about scability hence still use the integer
		ArrayList<Integer> types = new ArrayList<>(); 

		for (String s : keyAfterSplit) {
			if (curDoc.isJsonObject() && curDoc.getAsJsonObject().has(s)) {
				if (!curDoc.getAsJsonObject().get(s).isJsonObject() && !curDoc.getAsJsonObject().get(s).isJsonArray())
					return null;
				types.add(0); 
				curDoc = curDoc.getAsJsonObject().get(s);
				continue;
			}

			if (curDoc.isJsonArray() && isInteger(s)) {
				int index = Integer.parseInt(s);
				int size = curDoc.getAsJsonArray().size();
				if ((index < 0 && index > size) || (!curDoc.getAsJsonArray().get(index).isJsonArray()
						&& !curDoc.getAsJsonArray().get(index).isJsonObject()))
					return null;
				types.add(1); 
				curDoc = curDoc.getAsJsonArray().get(index);
				continue;

			}
			return null;
		}

		// deal the last key
		if (curDoc.isJsonObject() && curDoc.getAsJsonObject().has(keyAfterSplit[keyAfterSplit.length - 1])) {
			types.add(0);
			value = curDoc.getAsJsonObject().get(keyAfterSplit[keyAfterSplit.length - 1]);
		}

		if (curDoc.isJsonArray() && isInteger(keyAfterSplit[keyAfterSplit.length - 1])) {
			int index = Integer.parseInt(keyAfterSplit[keyAfterSplit.length - 1]);
			int size = curDoc.getAsJsonArray().size();
			if (index < 0 && index > size)
				return null;
			types.add(1);
			value = curDoc.getAsJsonArray().get(index);
		}

		return reConstruct(value, keyAfterSplit, types);
	}
	// i think may be we can use a stack here but i am lazy to change
	private JsonObject reConstruct(JsonElement value, String[] keyAfterSplit, ArrayList<Integer> types) {
		if (value == null) 
			return null;
		
		JsonElement newValue = value;
		for (int j = keyAfterSplit.length - 1; j >= 0; j--) {
			String key = keyAfterSplit[j];
			if (types.get(j) == 0) { // Construct a JsonObject
				JsonObject temp = new JsonObject();
				temp.add(key, newValue);
				newValue = temp;
			} else { // Construct a JsonArray
				JsonArray temp = new JsonArray();
				temp.add(newValue);
				newValue = temp;
			}
		}
		return (JsonObject) newValue;
	}

	private boolean isInteger(String str) {
		try {
			Integer.parseInt(str);
		} catch (NumberFormatException | NullPointerException exception) {
			return false;
		}
		return true;
	}

	/**
	 * Returns true if there are more documents to be seen
	 */
	public boolean hasNext() {
		return cursor < this.documents.size();
	}

	/**
	 * Returns the next document
	 */
	public JsonObject next() {
		return this.documents.get(this.cursor++);
	}

	/**
	 * Returns the total number of documents
	 */
	public long count() {
		return this.documents.size();
	}
}
