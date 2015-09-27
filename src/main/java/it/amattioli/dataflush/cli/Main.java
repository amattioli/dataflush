package it.amattioli.dataflush.cli;

import it.amattioli.dataflush.Consumer;
import it.amattioli.dataflush.Producer;
import it.amattioli.dataflush.concurrent.ConcurrentConsumer;
import it.amattioli.dataflush.scripting.Script;

import java.io.File;
import java.util.Date;

public class Main {

	public static void main(String[] args) {
		Date start = new Date();
		try {
			Producer producer = createProducer(args);
			producer.produce();
		} catch(Exception e) {
			if (e.getMessage() != null) {
				System.err.println(e.getMessage());
			} else {
				e.printStackTrace();
			}
		}
		System.out.println("Finished in "+(new Date().getTime()-start.getTime())+"ms");
	}
	
	private static Producer createProducer(String[] args) throws Exception {
		DataflushCommandLine cmd = new DataflushCommandLine(args);
		Script script = new Script(new File(cmd.getScriptFileName()));
		Producer producer = script.getProducer();
		Consumer consumer = script.getConsumer();
		if (cmd.hasConcurrentOption()) {
			consumer = new ConcurrentConsumer(consumer);
		}
		producer.setConsumer(consumer);
		return producer;
	}

}
