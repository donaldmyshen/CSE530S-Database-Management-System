 
package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	private File file;
	private TupleDesc td;
	private int id;
	
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		//your code here
		this.file = f;
		this.td = type;
		this.id = f.hashCode();
		
	}
	
	public File getFile() {
		//your code here
		return this.file;
	}
	
	public TupleDesc getTupleDesc() {
		//your code here
		return this.td;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id) {
		//your code here
		byte[] data = new byte[HeapFile.PAGE_SIZE];
		long start = PAGE_SIZE * id; 
		int tId = this.getId();
		HeapPage newPage = null;
		
		try {
			// have to try-catch here though I don;t know why
			RandomAccessFile fileReader = new RandomAccessFile(this.file, "r");
			fileReader.seek(start);
			fileReader.read(data); // read the length of content, PAGE_SIZE here
			fileReader.close();
			// int id, byte[] data, int tableId
			newPage = new HeapPage(id, data, tId);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return newPage;
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		return this.id;
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 */
	public void writePage(HeapPage p) {
		//your code here
		byte[] data = p.getPageData();
		long start = PAGE_SIZE * p.getId();

		try {
			RandomAccessFile fileReader = new RandomAccessFile(this.file, "rw");
			fileReader.seek(start);
			fileReader.write(data); 
			fileReader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating
	 * a new page if all others are full. It then passes the tuple to this page to
	 * be stored. It then writes the page to disk (see writePage)
	 * 
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 */
	public HeapPage addTuple(Tuple t) {
		//your code here
		HeapPage page = null;

		int i = 0;
		// find empty slot from first page
		for (; i  < this.getNumPages(); i++) {
			page = this.readPage(i);
			//skip
			if (page == null) continue;
			
			int j = 0;
			for (; j < page.getNumSlots(); j++) {
				if (!page.slotOccupied(j)) {
					try {
						page.addTuple(t);
						break;
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			
			if (j < page.getNumSlots()) { 
				break;
			}
		}
		// create a new page if all page full
		if (i == this.getNumPages()) {
				try {
					page = new HeapPage(this.getNumPages(), new byte[PAGE_SIZE], this.getId());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
				try {
					page.addTuple(t);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		
		this.writePage(page);
		return page;
	}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 */
	public void deleteTuple(Tuple t){
		//your code here
		HeapPage p = this.readPage(t.getPid());
		p.deleteTuple(t);
		this.writePage(p);
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		List<Tuple> list = new ArrayList<>();
		for (int i = 0; i < this.getNumPages(); i ++) {
			Iterator<Tuple> iter= this.readPage(i).iterator();
			while (iter.hasNext()) list.add(iter.next());
		}
		return (ArrayList<Tuple>) list;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		// cast double to int
		return (int) Math.ceil(this.file.length() /  PAGE_SIZE);
	}
}
