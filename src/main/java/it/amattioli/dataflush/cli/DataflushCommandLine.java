package it.amattioli.dataflush.cli;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class DataflushCommandLine {
	private CommandLine cmd;
	
	public DataflushCommandLine(String... args) throws ParseException {
		Options options = new Options();
		options.addOption("c","concurrent",false,"Use concurrent consumer");
		CommandLineParser parser = new BasicParser();
		cmd = parser.parse( options, args);
	}
	
	public boolean hasConcurrentOption() {
		return cmd.hasOption("c");
	}
	
	public String getScriptFileName() {
		if (cmd.getArgs().length == 0) {
			throw new RuntimeException("No script file specified.");
		}
		return cmd.getArgs()[0];
	}
}
