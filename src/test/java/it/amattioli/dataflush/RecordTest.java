package it.amattioli.dataflush;

import junit.framework.TestCase;

public class RecordTest extends TestCase {
	
	public void testGetAndSet() {
		Record r = new Record();
		String value = "Hello";
		r.set(5, value);
		assertEquals(value, r.get(5));
	}
	
	public void testEquals() {
		String value = "Hello";
		Record r1 = new Record();
		r1.set(0, value);
		Record r2 = new Record();
		r2.set(0, value);
		assertEquals(r1,r2);
	}
	
	public void testDifferent() {
		String value1 = "Hello1";
		Record r1 = new Record();
		r1.set(0, value1);
		String value2 = "Hello2";
		Record r2 = new Record();
		r2.set(0, value2);
		assertFalse(r1.equals(r2));
	}
	
}
