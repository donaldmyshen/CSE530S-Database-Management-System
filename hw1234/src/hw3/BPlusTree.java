 
package hw3;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

import hw1.Field;

public class BPlusTree {

	private Node root;
	private int pInner;
	private int pLeaf;
	static boolean flag = false;

	// input are degrees
	public BPlusTree(int pInner, int pLeaf) {
		this.pInner = pInner;
		this.pLeaf = pLeaf;
		this.root = new LeafNode(pLeaf);
	}

	public LeafNode search(Field f) {
		Node curNode = root;
		while (!curNode.isLeafNode())
			curNode = ((InnerNode) curNode).findChildByKey(f);
		return ((LeafNode) curNode).containField(f) ? (LeafNode) curNode : null;
	}

	public void insert(Entry e) {
		// use a recursive function overload
		insert(root, e);
		if (root.checkFull())
			split(root, null);
	}

	private void insert(Node node, Entry e) {
		// base Case, node is leaf
		if (node.isLeafNode()) {
			((LeafNode) node).addEntry(e);
		} else {
			InnerNode innerNode = (InnerNode) node;
			Node childNode = innerNode.findChildByKey(e.getField());
			insert(childNode, e);
			if (childNode.checkFull())
				split(childNode, innerNode);
			innerNode.refreshKeys();
		}
	}

	private void split(Node current, InnerNode parent) {
		Node newNode = null;
		if (current.isLeafNode()) {
			LeafNode newLeaf = new LeafNode(this.pLeaf);
			leafSplit((LeafNode) current, newLeaf);
			newNode = newLeaf;
		} else {
			InnerNode newInner = new InnerNode(this.pInner);
			innerSplit((InnerNode) current, newInner);
			newNode = newInner;
		}

		if (parent == null) {
			InnerNode newRoot = new InnerNode(this.pInner);
			newRoot.addChild(current);
			newRoot.addChild(newNode);
			root = newRoot;
		} else {
			parent.addChild(newNode);
		}
	}
	// hard to deal the input type, try to add as abstract class method but failed
	private void leafSplit(LeafNode curLeafNode, LeafNode newLeafNode) {
		ArrayList<Entry> entries = curLeafNode.getEntries();
		int mid = (int) Math.ceil(entries.size() / 2.0);
		curLeafNode.entries = new ArrayList<>(entries.subList(0, mid));
		newLeafNode.entries = new ArrayList<>(entries.subList(mid, entries.size()));
	}

	private void innerSplit(InnerNode curInnerNode, InnerNode newInnerNode) {
		ArrayList<Node> children = curInnerNode.getChildren();
		int mid = (int) Math.ceil(children.size() / 2.0);
		curInnerNode.children = new ArrayList<>(children.subList(0, mid));
		curInnerNode.refreshKeys();
		newInnerNode.children = new ArrayList<>(children.subList(mid, children.size()));
		newInnerNode.refreshKeys();
	}

	public Node getRoot() {
		// almost forget this one until keep testing failure
		if (this.root.isLeafNode() && ((LeafNode) root).entries.size() == 0)
			return null;
		return this.root;
	}

	public void delete(Entry e) {
		LeafNode searchNode = this.search(e.getField());
		if (searchNode == null)
			return;
		else {
			Deque<Node> stack = new ArrayDeque<Node>();
			stack.push(root);
			delete(stack, e);
		}
	}

	private void delete(Deque<Node> stack, Entry e) {
		Node curNode = stack.peek();
		// base case, in leaf
		if (curNode.isLeafNode()) {
			((LeafNode) curNode).removeEntry(e);
			
			if (((LeafNode) curNode).entries.size() == 0) {
				//System.out.println(curNode.equals(root));
				curNode = null;
			}
		}
		else {
			Node child = ((InnerNode) curNode).findChildByKey(e.getField());
			stack.push(child);
			delete(stack, e);
		}
		delteHelper(stack);
		stack.pop();
		if (flag && stack.size() > 0)
			((InnerNode) stack.peek()).refreshKeys();
	}

	private void delteHelper(Deque<Node> stack) {
		Node curNode = stack.peek();
		
		if (root.equals(curNode)) {
			if (curNode.isLeafNode())
				return;
			else {
				if (((InnerNode) root).getChildren().size() < 2) 
					root = ((InnerNode) root).getChildren().get(0);
				return;
			}
		}

		if (!curNode.checkHalf()) {
			flag = true;
			stack.pop();
			InnerNode parrentNode = (InnerNode) stack.peek();
			stack.push(curNode);

			Node sibNode = this.getLeftSibling(stack) == null ? this.getRightSibling(stack)
					: this.getLeftSibling(stack);

			if (sibNode.isLeafNode()) {
				LeafNode temp = (LeafNode) sibNode;
				if (temp.entries.size() == (int) Math.ceil(temp.getDegree() / 2.0)) {
					for (Entry e : ((LeafNode) curNode).getEntries())
						((LeafNode) sibNode).addEntry(e);
					parrentNode.removeChild(curNode);
				}
				// borrow
				else {
					ArrayList<Entry> sibEntries = ((LeafNode) sibNode).getEntries();
					Entry burrowEntry = sibEntries.get(sibEntries.size() - 1);
					((LeafNode) sibNode).removeEntry(burrowEntry);
					((LeafNode) curNode).addEntry(burrowEntry);
				}

			} else {
				InnerNode temp = (InnerNode) sibNode;
				if (temp.keys.size() == (int) Math.ceil(temp.getDegree() / 2.0)) {
					for (Node n : ((InnerNode) curNode).getChildren())
						((InnerNode) sibNode).addChild(n);
					parrentNode.removeChild(curNode);
				}
				// borrow
				else {
					ArrayList<Node> sibChildren = ((InnerNode) sibNode).getChildren();
					Node burrowNode = sibChildren.get(sibChildren.size() - 1);
					((InnerNode) sibNode).removeChild(burrowNode);
					((InnerNode) curNode).addChild(burrowNode);
				}
			}
		} else {
			// don't need to merge
			flag = false;
		}
	}

	private Node getLeftSibling(Deque<Node> stack) {
		ArrayDeque<Node> deque = ((ArrayDeque<Node>) stack).clone();
		Node sib = null;
		int index = 0;
		while (deque.size() >= 2) {
			Node child = deque.pop();
			sib = ((InnerNode) deque.peek()).getLeftSibling(child);
			if (sib == null) {
				index++;
			} else {
				for (int i = 1; i <= index; i++) {
					ArrayList<Node> children = ((InnerNode) sib).getChildren();
					sib = children.get(children.size() - 1);
				}
				return sib;
			}
		}
		return sib;
	}

	private Node getRightSibling(Deque<Node> stack) {
		ArrayDeque<Node> deque = ((ArrayDeque<Node>) stack).clone();
		Node sib = null;
		int index = 0; 
		while (deque.size() >= 2) {
			Node child = deque.pop();
			sib = ((InnerNode) deque.peek()).getRightSibling(child);
			if (sib == null) {
				index++;
			} else {
				for (int i = 1; i <= index; i++) {
					sib = ((InnerNode) sib).children.get(0);
				}
				return sib;
			}
		}
		return sib;
	}
}
