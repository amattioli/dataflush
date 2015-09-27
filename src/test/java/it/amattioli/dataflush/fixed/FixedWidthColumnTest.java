package it.amattioli.dataflush.fixed;

import junit.framework.TestCase;

public class FixedWidthColumnTest extends TestCase {

	public void testWriteStringValue() {
		FixedWidthColumn col = new FixedWidthColumn(5, 9);
		StringBuffer line = new StringBuffer("           ");
		col.writeValue(line, "abcde");
		assertEquals("     abcde ", line.toString());
	}
	
	public void testWriteLongerValue() {
		FixedWidthColumn col = new FixedWidthColumn(5, 8);
		StringBuffer line = new StringBuffer("           ");
		col.writeValue(line, "abcde");
		assertEquals("     abcd  ", line.toString());
	}
	
	public void testWriteShorterValue() {
		FixedWidthColumn col = new FixedWidthColumn(5, 10);
		StringBuffer line = new StringBuffer("           ");
		col.writeValue(line, "abcde");
		assertEquals("     abcde ", line.toString());
	}
	
}
