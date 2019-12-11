//By Mingyuan Shen and Jacob Li
package hw2;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import hw1.Field;
import hw1.IntField;
import hw1.StringField;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	AggregateOperator ao;
	boolean gby;
	TupleDesc tupDs;
	ArrayList<Tuple> tuple = new ArrayList<Tuple>();
	ArrayList<Tuple> compare = new ArrayList<Tuple>();

	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		ao = o;
		gby = groupBy;
		tupDs = td;
	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		//your code here
		Type one = t.getDesc().getType(0);
		Field two = null;
		tuple.add(t);
		if(gby == true){
			//String case
			if(one == Type.STRING){
				compare.add(mergeHelper(t, two, one));			
			}else{
				//Int case
				if(compare.contains(mergeHelper(t, two, one))){
					
				}else{
					compare.add(mergeHelper(t, two, one));
				}
			}
		}
	}
	
	private Tuple mergeHelper(Tuple t, Field two, Type one) {
		byte[] a = t.getField(0).toByteArray();
		//two case for Int and String
		if(one == Type.STRING) {
			two = new StringField(a);
		}else {
			two = new IntField(a);
		}
		Tuple newTuple = new Tuple(tupDs);
		newTuple.setField(0, two);
		return newTuple;
	}
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		//use arraylist to storage the answer
		ArrayList<Tuple> answer = new ArrayList<Tuple>();
		if(ao == AggregateOperator.COUNT){
			//if group by
			if(gby == false){
				int ans = 0;
				int len = tuple.size();
				for(int i=0; i<len; i++){
					ans++;
				}
				answer.add(countHelper1(ans));
			}else{//no group by
				for(int i=0; i<compare.size(); i++) {
					int ans = 0;
					for(int j=0; j<tuple.size(); j++) {
						if(tuple.get(j).equals(compare.get(i))) {
							ans++;
						}
						answer.add(countHelper(i, ans));
					}
				}
			}
		}
		
		if(ao == AggregateOperator.SUM) {
			int sum = 0;
			if(gby == true) {
				//Use a arraylist to store the answer
				ArrayList<Tuple> list = new ArrayList<>();
				Type[] types = new Type[] { tupDs.getType(0), Type.INT };
				String[] fields = new String[] { tupDs.getFieldName(0), "SUM" };
				TupleDesc newTd = new TupleDesc(types, fields);
				// Judge type
				if (tupDs.getType(0) == Type.INT && tupDs.getType(1) == Type.INT) {
					Map<Integer, Integer> name = new HashMap<>(); // First int represents name, sencond int represents index
					for(int i=0; i<tuple.size(); i++) {
						//Judge whether the name in the Map or not
						if (name.containsKey(tuple.get(i).getField(0).hashCode())) {
							sumHelperInt(tuple.get(i), name, list);
						} else {
							sumHelperInt1(i, list, name, newTd);
						}
					}
				} else if (tupDs.getType(0) == Type.STRING && tupDs.getType(1) == Type.INT) {
					Map<String, Integer> name = new HashMap<>(); //The String represents name, the Int represents index
					for (int i=0; i<tuple.size(); i++) {
						//Judge whether the name in the Map or not
						if (name.containsKey(tuple.get(i).getField(0).toString())) {
							sumHelperString(tuple.get(i), name, list);
						} else {
							sumHelperString1(i, list, name, newTd);
						}
					}
				}
				return list;
			}else {		
				for(int i=0; i<tuple.size(); i++) {
					Type a = tuple.get(i).getDesc().getType(0);
					//Judge Type
					if(!a.equals(Type.STRING)) {
						int value = ((IntField) tuple.get(i).getField(0)).getValue();
						sum = sum + value;
					}
				}
				answer.add(countHelper1(sum));
			}
		}

		if(ao == AggregateOperator.AVG) {
			//group by case
			if (gby == true) {
				for(int i=0; i<compare.size(); i++) {
					int sum = 0;
					int size = 0;
					int avg = 0;
					Type a = compare.get(i).getDesc().getType(0);
					//Judge type
					if(!a.equals(Type.STRING)) {
						IntField intValue = (IntField) compare.get(i).getField(0);
						int actValue = intValue.getValue();
						for(int j=0; j<tuple.size(); j++) {
							Type b = tuple.get(j).getDesc().getType(0);
							if(b == Type.INT) {
								IntField intValue1 = (IntField) tuple.get(j).getField(0);
								int actValue1 = intValue1.getValue();
								if(actValue1 == actValue) {
									IntField comValue = (IntField) tuple.get(j).getField(1);
									int valb = comValue.getValue();
									sum = sum + valb;
									size++;
								}
							}
						}
					}
					avg = sum/size;
					answer.add(countHelper(i, avg));
				}
			} else {//no group by
				int sum = 0;
				int avg = 0;
				Tuple ans = new Tuple(tupDs);
				//calculate the sum first
				for(int i=0; i<tuple.size(); i++) {
					int temp = tuple.get(i).getField(0).hashCode();
					sum = sum + temp;
				}
				//calculate the average value then
				avg = sum / tuple.size();
				IntField intfield = new IntField(avg);
				ans.setField(0, intfield);
				answer.add(ans);
			}
		}
		
		if(ao == AggregateOperator.MAX) {
			//group by case
			if(gby == false) {
				int max = Integer.MIN_VALUE;
				boolean ifInt = false;
				String stringVal = "";
				Type a = tuple.get(0).getField(0).getType();
				if(a.equals(Type.INT)) {
					//Int type, cool
					ifInt = true;
				}
				if(ifInt == false) {
					//transfer the value to String type
                    stringVal = ((StringField) tuple.get(0).getField(0)).toString();
				}
				for(int i=0; i<tuple.size(); i++) {
					Type b = tuple.get(i).getDesc().getType(0);
					if(b.equals(Type.STRING)) {
						valueHelper(i, stringVal);
					}else {
						IntField intField = new IntField(tuple.get(i).getField(0).toByteArray());
						int actValue1 = intField.getValue();
						if(max < actValue1) {
							max = actValue1;
						}
					}
				}
				//Judge and return the right type
				if(ifInt == true) {
					answer.add(maxMinInt(max));
				}else {
					answer.add(maxMinString(stringVal));
				}
			}else {
				int max = Integer.MIN_VALUE;
				boolean ifInt = false;
				String firstValue = "";
				Type a = tuple.get(0).getField(0).getType();
				if(a.equals(Type.INT)) {
					//int type, cool
					ifInt = true;
				}
				if(ifInt == false) {
					//transfer the value to String
                    firstValue = ((StringField) tuple.get(0).getField(0)).toString();
				}
				for(int i=0; i<compare.size(); i++) {
					IntField value = (IntField) compare.get(i).getField(0);
					int actValue = value.getValue();
					for(int j=0; j<tuple.size(); j++) {
						Type b = tuple.get(j).getDesc().getType(0);
						if(b == Type.INT) {//int case
							IntField intValue = (IntField) tuple.get(j).getField(0);
                            int actValue1 = intValue.getValue();
                            if(actValue1 == actValue) {
                            	IntField intField = (IntField) tuple.get(j).getField(1);
                            	int valuec = intField.getValue();
                            	if(max < valuec) {
                            		max = valuec;
                            	}
                            }
						}else {//string case
							stringHelper(j, firstValue);
						}
					}
					//Judge and return the right type
					if(ifInt == true) {
						answer.add(maxMinIntHelper(i, max));
					}else {
						answer.add(maxMinStringHelper(i, firstValue));
					}
				}
			}
		}
		
		if(ao == AggregateOperator.MIN) {
			if(gby == false) {
				int min = Integer.MAX_VALUE;
				boolean ifInt = false;
				String firstValue = "";
				Type a = tuple.get(0).getField(0).getType();
				if(a.equals(Type.INT)) {
					//int type, cool
					ifInt = true;
				}
				if(ifInt == false) {
					//transfer the value to String
                    firstValue = ((StringField) tuple.get(0).getField(0)).toString();
				}
				for(int i=0; i<tuple.size(); i++) {
					Type b = tuple.get(i).getDesc().getType(0);
					if(b.equals(Type.STRING)) {//String case
						valueHelper(i, firstValue);
					}else {//int case
						IntField intField = new IntField(tuple.get(i).getField(0).toByteArray());
						int actValue1 = intField.getValue();
						if(min > actValue1) {
							min = actValue1;
						}
					}
				}
				//Judge and return the right type
				if(ifInt == true) {
					answer.add(maxMinInt(min));
				}else {
					answer.add(maxMinString(firstValue));
				}
			}else {
				int min = Integer.MAX_VALUE;
				boolean ifInt = false;
				String firstValue = "";
				Type a = tuple.get(0).getField(0).getType();
				if(a.equals(Type.INT)) {
					//int value, cool
					ifInt = true;
				}
				if(ifInt == false) {
					//transfer to String
                    firstValue = ((StringField) tuple.get(0).getField(0)).toString();
				}
				for(int i=0; i<compare.size(); i++) {
					IntField value = (IntField) compare.get(i).getField(0);
					int actValue = value.getValue();
					for(int j=0; j<tuple.size(); j++) {
						Type b = tuple.get(j).getDesc().getType(0);
						if(b == Type.INT) {//int case
							IntField intValue = (IntField) tuple.get(j).getField(0);
                            int actValue1 = intValue.getValue();
                            if(actValue1 == actValue) {
                            	IntField intField = (IntField) tuple.get(j).getField(1);
                            	int valuec = intField.getValue();
                            	if(min > valuec) {
                            		min = valuec;
                            	}
                            }
						}else {//String case
							stringHelper(j, firstValue);
						}
					}//Judge
					if(ifInt == true) {
						answer.add(maxMinIntHelper(i, min));
					}else {
						answer.add(maxMinStringHelper(i, firstValue));
					}
				}
			}
		}
		return answer;
	}
	
	//helpers used in this class
	private Tuple maxMinIntHelper(int i, int maxMin){
		Tuple maxminTuple = new Tuple(tupDs);
		maxminTuple.setField(0, compare.get(i).getField(0));
		IntField maxminInt = new IntField(maxMin);
		maxminTuple.setField(1, maxminInt);
		return maxminTuple;
	}
	
	private Tuple maxMinStringHelper(int i, String firstValue) {
		Tuple maxminTuple = new Tuple(tupDs);
		maxminTuple.setField(0, compare.get(i).getField(0));
		StringField maxminString = new StringField(firstValue);
		maxminTuple.setField(1, maxminString);
		return maxminTuple;
	}	
	
	private void stringHelper(int j, String firstValue) {
		StringField stringField = (StringField) tuple.get(j).getField(1);
		String actStringValue = stringField.toString();
		if(actStringValue.compareTo(firstValue)==1) firstValue = actStringValue;
	}
	
	private void valueHelper(int i, String firstValue) {
		StringField value = (StringField) tuple.get(i).getField(0);
		String actValue = value.toString();
		if(actValue.compareTo(firstValue) == 1) {
			firstValue = actValue;
		}
	}
	
	private Tuple maxMinInt(int maxmin) {
		Tuple maxminTuple = new Tuple(tupDs);
		IntField intMaxmin = new IntField(maxmin);
		maxminTuple.setField(0, intMaxmin);
		return maxminTuple;
	}
	
	private Tuple maxMinString(String firstValue) {
		Tuple maxminTuple = new Tuple(tupDs);
		StringField stringMaxmin = new StringField(firstValue);
		maxminTuple.setField(0, stringMaxmin);
		return maxminTuple;
	}
	
	private Tuple countHelper(int i, int ans) {
		Tuple newTuple = new Tuple(tupDs);
		newTuple.setField(0, compare.get(i).getField(0));
		IntField b = new IntField(ans);
		newTuple.setField(1, b);
		return newTuple;
	}
	
	private Tuple countHelper1(int ans) {
		Tuple newTuple = new Tuple(tupDs);
		IntField b = new IntField(ans);
		newTuple.setField(0, b);
		return newTuple;
	}
	
	private void sumHelperInt(Tuple t, Map<Integer, Integer> name, ArrayList<Tuple> list) {
		int index = name.get(t.getField(0).hashCode());
		Tuple temp = list.get(index);
		IntField intf = new IntField(t.getField(1).hashCode() + temp.getField(1).hashCode());
		temp.setField(1, intf);
	}
	
	private void sumHelperString(Tuple t, Map<String, Integer> nameSpace, ArrayList<Tuple> list) {
		int index = nameSpace.get(t.getField(0).hashCode());
		Tuple temp = list.get(index);
		IntField intf = new IntField(t.getField(1).hashCode() + temp.getField(1).hashCode());
		temp.setField(1, intf);
	}
	
	private void sumHelperInt1(int i, ArrayList<Tuple> list, Map<Integer, Integer> name, TupleDesc newTd) {
		Tuple newTuple = new Tuple(newTd);
		newTuple.setField(0, tuple.get(i).getField(0));
		newTuple.setField(1, tuple.get(i).getField(1));
		list.add(newTuple);
		name.put(newTuple.getField(0).hashCode(), list.size() - 1);
	}
	
	private void sumHelperString1(int i, ArrayList<Tuple> list, Map<String, Integer> name, TupleDesc newTd) {
		Tuple newTuple = new Tuple(newTd);
		newTuple.setField(0, tuple.get(i).getField(0));
		newTuple.setField(1, tuple.get(i).getField(1));
		list.add(newTuple);
		name.put(newTuple.getField(0).toString(), list.size() - 1);
	}
}