 
package hw2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import hw1.Field;
import hw1.RelationalOperator;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		this.tuples = l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		if (field < 0 || field >= td.numFields() || op == null || operand == null) return null;
		ArrayList<Tuple> list = new ArrayList<>();
		
		for (Tuple t: this.tuples) {
			if (t.getField(field).compare(op, operand)) list.add(t);
		}
		this.tuples = list;
		
		return this;
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 * @throws Exception 
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) throws Exception {
		//your code here
		Type[] tdTypes = td.copyTypes();
		String[] tdFields = td.copyFields();
		//System.out.println(Arrays.asList(tdFields).toString());
		//System.out.println(names.toString());
		// handle duplicate rename
		
		for (String s : tdFields) {
			if (names.contains(s)) {
				//System.out.println(true);
				throw new Exception();
			}
			
		}

		for (int i = 0; i < fields.size(); i++) {
			if (fields.get(i) != null && !names.get(i).equals("")){
				String name = names.get(i);
				tdFields[fields.get(i)] = name;
			}
		}
		TupleDesc newTd = new TupleDesc(tdTypes, tdFields);
		ArrayList<Tuple> newTuples = tuples;
		for (Tuple t: newTuples) t.setDesc(newTd);
		return new Relation(newTuples, newTd);
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here
		
		Type[] newTypes = new Type[fields.size()];
		String[] newFields = new String[fields.size()];
		TupleDesc newTd = new TupleDesc(newTypes, newFields);
		ArrayList<Tuple> newTuples = new ArrayList<>();
		
		// edge case
		if (fields.size() == 0) return new Relation(newTuples, newTd);
		@SuppressWarnings("unchecked")
		ArrayList<Integer> temp = (ArrayList<Integer>) fields.clone();
		temp.sort(null);
		if (temp.get(temp.size() - 1) > td.numFields() || temp.get(temp.size() - 1) > td.numTypes()) {
			return new Relation(tuples, td);
		}
		//System.out.println(temp.toString());
		for (int i = 0; i < fields.size(); i++) {
			try {
				//System.out.println(fields.get(i));
				newTypes[i] = td.getType(fields.get(i));
				newFields[i] = td.getFieldName(fields.get(i));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				throw new IndexOutOfBoundsException();
			}
		}
		
		for (Tuple t: tuples) {
			Tuple newT = new Tuple(newTd);
			for (int i = 0; i < fields.size(); i++) {
				try {
					newT.setField(i, t.getField(fields.get(i)));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					throw new IndexOutOfBoundsException();
				}
			}
			newTuples.add(newT);
			//System.out.println(newTuples.size()+" 1");
		}
		return new Relation(newTuples, newTd);
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		//your code here
		String[] fields1 = td.copyFields();
		String[] fields2 = other.td.copyFields();
		
		Type[] types1 = td.copyTypes();
		Type[] types2 = other.td.copyTypes();
		//http://www.java2s.com/Code/Java/Collections-Data-Structure/Appendonearraytoanother.htm
		String[] fields = Arrays.copyOf(fields1, fields1.length + fields2.length);
		System.arraycopy(fields2, 0, fields, fields1.length, fields2.length); 
		Type[]types = Arrays.copyOf(types1, types1.length + types2.length);
		System.arraycopy(types2, 0, types, types1.length, types2.length);
		
		TupleDesc newTd = new TupleDesc(types, fields);
		ArrayList<Tuple> newTuples = new ArrayList<>();
		for (Tuple t1: tuples) {
			for (Tuple t2: other.tuples) {
				if (t1.getField(field1).compare(RelationalOperator.EQ, t2.getField(field2))) {
					Tuple newTuple = new Tuple(newTd);
					for (int i = 0; i < fields1.length; i++) newTuple.setField(i, t1.getField(i));
					for (int i = 0; i < fields2.length; i++) newTuple.setField(i, t2.getField(i));
					newTuples.add(newTuple);
				}
			}
		}
		return new Relation(newTuples, newTd);
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		Aggregator aggre = new Aggregator(op, groupBy, this.td);
		
		for(Tuple t: this.tuples) {
			aggre.merge(t);
		}
		
		ArrayList<Tuple> newTuples = aggre.getResults();
		Relation res = new Relation(newTuples, newTuples.get(0).getDesc());
		return res;
	}
	
	public TupleDesc getDesc() {
		//your code here
		return td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		//return null;
		String s = td.toString() + '\n';
		for (Tuple t : tuples) s += t.toString() + '\n';
		return s;
	}
}
