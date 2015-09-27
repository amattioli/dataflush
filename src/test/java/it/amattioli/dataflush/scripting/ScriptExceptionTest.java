package it.amattioli.dataflush.scripting;

import junit.framework.TestCase;

public class ScriptExceptionTest extends TestCase {

	public void testNestedCause() {
		ScriptException e = new ScriptException(10, new IllegalArgumentException(new RuntimeException("Target message")));
		assertEquals("Syntax error in line 10: Target message", e.getMessage());
	}
	
}
