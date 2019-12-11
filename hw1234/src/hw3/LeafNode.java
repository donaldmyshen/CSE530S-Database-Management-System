 
package hw3;

import java.util.ArrayList;

import hw1.Field;
import hw1.RelationalOperator;

public class LeafNode implements Node {
	private int degree;
	ArrayList<Entry> entries;
	
	public LeafNode(int degree) {
		this.degree = degree;
		entries = new ArrayList<>();
	}
	
	public ArrayList<Entry> getEntries() {
		return entries;
	}

	public int getDegree() {
		return degree;
	}
	
	public boolean isLeafNode() {
		return true;
	}
	
	public boolean checkFull() {
		return entries.size() > degree;
	}
	
	public boolean checkHalf() {
		return entries.size() >= (int)Math.ceil(degree / 2.0);
	}

	public void addEntry(Entry e) {
		for(Entry en : entries) {
			if (en.getField().compare(RelationalOperator.EQ, e.getField())) 
				return; 
			if (en.getField().compare(RelationalOperator.GT, e.getField())) {
				entries.add(entries.indexOf(en), e);
				return;
			}
		}
		entries.add(e);
	}
	
	public void removeEntry(Entry e) {
		for (Entry en : entries) {
			if (en.getField().compare(RelationalOperator.EQ, e.getField())) {
				entries.remove(en);
				return;
			}
		}
	}
	
	public boolean containField(Field field) {
		for (Entry e : entries) {
			if (e.getField().compare(RelationalOperator.EQ, field)) {
				return true;
			}
		}
		return false;
	}
	
	public Field getSearchKey() {
		return entries.get(entries.size() - 1).getField();
	}
}