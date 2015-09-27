package it.amattioli.dataflush.concurrent;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Consumer;
import it.amattioli.dataflush.Record;

public class SlowConcurrentConsumer extends Consumer {
	private Consumer decorated;
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	private Throwable executionException = null;
	
	public SlowConcurrentConsumer(Consumer decorated) {
		this.decorated = decorated;
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
		executor.submit(new Runnable() {
			
			public void run() {
				try {
					decorated.consume(record);
				} catch(Throwable e) {
					setExecutionException(e);
				}
			}
			
		});
		if (record == null) {
			executor.shutdown();
		}
	}
	
	private synchronized Throwable getExecutionException() {
		return executionException;
	}
	
	private synchronized void setExecutionException(Throwable e) {
		this.executionException = e;
		executor.shutdownNow();
	}

}
