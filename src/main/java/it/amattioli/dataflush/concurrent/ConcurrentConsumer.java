package it.amattioli.dataflush.concurrent;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Consumer;
import it.amattioli.dataflush.Record;

public class ConcurrentConsumer extends Consumer {
	private Consumer decorated;
	private BlockingQueue<Record> recordQueue = new ArrayBlockingQueue<Record>(10);
	private Thread consumingThread = new Thread(new Runnable() {
		
		public void run() {
			try {
				while(true) {
					try {
						Record record = recordQueue.take();
						decorated.consume(record);
						if (record == Record.NULL) {
							return;
						}
					} catch(InterruptedException e) {
						return;
					}
				}
			} catch(Throwable t) {
				setExecutionException(t);
			}
		}
	});
	private Throwable executionException = null;
	
	public ConcurrentConsumer(Consumer decorated) {
		this.decorated = decorated;
		consumingThread.start();
	}
	
	@Override
	public Column createColumn() {
		return decorated.createColumn();
	}

	@Override
	public void addColumn(Column column) {
		decorated.addColumn(column);
	}

	@Override
	public List getColumns() {
		return decorated.getColumns();
	}

	@Override
	public void consume(final Record record) {
		if (getExecutionException() != null) {
			throw new RuntimeException(getExecutionException());
		}
		try {
			recordQueue.put(record);
		} catch(InterruptedException e) {
			
		}
	}
	
	private synchronized Throwable getExecutionException() {
		return executionException;
	}
	
	private synchronized void setExecutionException(Throwable e) {
		this.executionException = e;
	}

}
