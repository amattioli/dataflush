package it.amattioli.dataflush.testutils;

import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.io.IOUtils;

public class FileAssert {

	public static void assertSameContent(String expectedResource, StringWriter writer) throws Exception {
		Reader expectedReader = new InputStreamReader((FileAssert.class).getResourceAsStream(expectedResource));
		StringReader actualReader = new StringReader(writer.toString());
		assertSameContent(expectedReader, actualReader);
	}
	
	public static void assertSameContent(String expected, String actual) throws Exception {
		Reader expectedReader = new FileReader(expected);
		Reader actualReader = new FileReader(actual);
		assertSameContent(expectedReader, actualReader);
	}
	
	public static void assertSameContent(Reader expected, Reader actual) throws Exception {
		List expectedLines = IOUtils.readLines(expected);
		List actualLines = IOUtils.readLines(actual);
		TestCase.assertEquals(expectedLines, actualLines);
	}
	
}
