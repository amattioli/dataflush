package it.amattioli.dataflush.cli;

import it.amattioli.dataflush.testutils.FileAssert;

import java.io.File;

import org.apache.commons.io.FileUtils;

import junit.framework.TestCase;

public class MainTest extends TestCase {

	public void testCsvToCsv() throws Exception {
		String producedFile = "target/dest.csv";
		String expectedFile = "src/test/resources/it/amattioli/dataflush/cli/expected.csv";
		String scriptFile = "src/test/resources/it/amattioli/dataflush/cli/csvToCsv.script";
		FileUtils.deleteQuietly(new File(producedFile));
		Main.main(new String[] {scriptFile});
		//Thread.sleep(1000);
		FileAssert.assertSameContent(producedFile, expectedFile);
	}

}
