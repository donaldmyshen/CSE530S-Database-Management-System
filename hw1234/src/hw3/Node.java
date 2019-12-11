 
package hw3;

import hw1.Field;

public interface Node {
	// why not abstarct class?
	public int getDegree();
	public boolean isLeafNode();
	public Field getSearchKey();
	public boolean checkFull();
	public boolean checkHalf();
	
}
