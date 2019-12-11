 
package hw3;

import java.util.ArrayList;

import hw1.Field;
import hw1.RelationalOperator;

public class InnerNode implements Node {
	
	ArrayList<Field> keys; 			
	ArrayList<Node> children;		
	private int degree;				
	
	public InnerNode(int degree) {
		this.degree = degree - 1;
		this.keys = new ArrayList<>(); 		
		this.children = new ArrayList<>(); 	
	}
	
	public ArrayList<Field> getKeys() {
		return keys;
	}
	
	public ArrayList<Node> getChildren() {
		return children;
	}

	public int getDegree() {
		return degree;
	}
	
	public boolean isLeafNode() {
		return false;
	}
	
	public boolean checkFull() {
		return keys.size() > degree;
	}
	
	public boolean checkHalf() {
		return keys.size() >= (int)Math.ceil(degree/2.0);
	}
	
	public void refreshKeys() {
		ArrayList<Field> newKeys = new ArrayList<>();
		for (int i = 0; i < children.size() - 1; i++) {
			newKeys.add(children.get(i).getSearchKey());
		}
		this.keys = newKeys;
	}
	
	public void addChild(Node node) {
		Field key = node.getSearchKey();
		for (Node n : children) {
			if (n.getSearchKey().compare(RelationalOperator.GTE, key)) {
				children.add(children.indexOf(n), node);
				refreshKeys();
				return;
			}
		}
		children.add(node);
		refreshKeys();
	}
	
	public void removeChild(Node node) {
		children.remove(node);
		refreshKeys();
	}
	
	public Node findChildByKey(Field key) {
		for (Field f : keys) {
			if (f.compare(RelationalOperator.GTE, key)) {
				return children.get(keys.indexOf(f));
			}
		}
		return children.get(keys.size());
	}
	
	public Node getLeftSibling(Node child) {
		return children.indexOf(child) == 0 ? null : children.get(children.indexOf(child) - 1);
	}
	
	public Node getRightSibling(Node child) {
		return children.indexOf(child) + 1 > children.size() ? null : children.get(children.indexOf(child) + 1);
	}

	public Field getSearchKey() {
		return children.get(children.size() - 1).getSearchKey();
	}
}