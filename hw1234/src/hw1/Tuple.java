 
package hw1;

import java.sql.Types;
import java.util.HashMap;

/**
 * This class represents a tuple that will contain a single row's worth of information
 * from a table. It also includes information about where it is stored
 * @author Sam Madden modified by Doug Shook
 *
 */
public class Tuple {
	
	/**
	 * Creates a new tuple with the given description
	 * @param t the schema for this tuple
	 */
	private TupleDesc tupleDesc;
	private int pageId;
	private int slotId;
	private Field[] field;
	
	public Tuple(TupleDesc t) {
		//your code here
		this.tupleDesc = t;
		this.field = new Field[t.numFields()];
	}
	
	public TupleDesc getDesc() {
		//your code here
		return tupleDesc;
	}
	
	/**
	 * retrieves the page id where this tuple is stored
	 * @return the page id of this tuple
	 */
	public int getPid() {
		//your code here
		return pageId;
	}

	public void setPid(int pid) {
		//your code here
		this.pageId = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		//your code here
		return slotId;
	}

	public void setId(int id) {
		//your code here
		this.slotId = id;
	}
	
	public void setDesc(TupleDesc td) {
		//your code here;
		this.tupleDesc = td;
	}
	
	/**
	 * Stores the given data at the i-th field
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, Field v) {
		//your code here
		this.field[i] = v;
		//field[i] = v;
	}
	
	public Field getField(int i) {
		//your code here
		return field[i];
	}
	
	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
		//your code here
		// id convert to ints in base-10?
		StringBuffer sb = new StringBuffer();
		//sb.append(tupleDesc.toString());
		for (int i = 0; i < field.length; i ++) {
			sb.append(",");
			if (this.tupleDesc.getType(i) == Type.INT) sb.append((IntField)field[i]);
			else if (this.tupleDesc.getType(i) == Type.STRING) sb.append((StringField)field[i]);
		}
		sb.deleteCharAt(0);
		return sb.toString();
	}
}
	