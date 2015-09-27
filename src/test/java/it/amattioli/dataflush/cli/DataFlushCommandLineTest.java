package it.amattioli.dataflush.cli;

import junit.framework.TestCase;

public class DataFlushCommandLineTest extends TestCase {

	public void testFileName() throws Exception {
		DataflushCommandLine cmd = new DataflushCommandLine("myscript");
		assertEquals("myscript", cmd.getScriptFileName());
	}
	
	public void testFalseConcurrentOption() throws Exception {
		DataflushCommandLine cmd = new DataflushCommandLine("myscript");
		assertFalse(cmd.hasConcurrentOption());
	}
	
	public void testTrueConcurrentOption() throws Exception {
		DataflushCommandLine cmd = new DataflushCommandLine("-c", "myscript");
		assertTrue(cmd.hasConcurrentOption());
	}
	
	public void testLongConcurrentOption() throws Exception {
		DataflushCommandLine cmd = new DataflushCommandLine("--concurrent", "myscript");
		assertTrue(cmd.hasConcurrentOption());
	}
	
}
