package it.amattioli.dataflush;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.Format;
import java.text.NumberFormat;
import java.util.Locale;

import junit.framework.TestCase;

public class ColumnTest extends TestCase {

	public void testNumberFormat() {
		Column col = new Column();
		col.setType("number");
		col.setFormat("####");
		Format fmt = col.getFormatter();
		assertTrue(fmt instanceof NumberFormat);
	}
	
	public void testLocalizedNumberFormat() throws Exception {
		Column col = new Column();
		col.setType("number");
		col.setFormat("0.000");
		col.setLocale(Locale.ENGLISH);
		Format fmt = col.getFormatter();
		assertEquals(new Double("0.2000"), fmt.parseObject("0.2000"));
	}
	
	public void testDateFormat() {
		Column col = new Column();
		col.setType("date");
		col.setFormat("dd/MM/yyyy");
		Format fmt = col.getFormatter();
		assertTrue(fmt instanceof DateFormat);
	}
	
	public void testStringFormat() {
		Column col = new Column();
		col.setType("string");
		Format fmt = col.getFormatter();
		assertNull(fmt);
	}
	
	public void testNullTypeFormat() {
		Column col = new Column();
		Format fmt = col.getFormatter();
		assertNull(fmt);
	}
	
}
