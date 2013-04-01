package uk.co.connorhd.util.concurrent;

class TaskThread extends Thread {
	private Runnable task = null;
	private TaskThreadPool pool;
	private boolean started = false;

	public TaskThread(TaskThreadPool pool) {
		super("TaskThread");
		this.pool = pool;
	}

	@Override
	public synchronized void run() {
		started = true;
		boolean timedOut = false;
		while (true) {
			if (pool.hasShutdown()) {
				break;
			}
			if (task != null) {
				timedOut = false;
				task.run();
				task = pool.getTaskForThread(this);
				continue;
			}
			if (timedOut) {
				if (pool.threadWillEnd(this)) {
					break;
				} else {
					try {
						this.wait();
						continue;
					} catch (InterruptedException e) {
						continue;
					}
				}
			}
			if (pool.keepAlive > 0) {
				try {
					this.wait(pool.keepAlive);
					timedOut = true;
				} catch (InterruptedException e) {
					continue;
				}
			}
		}
	}

	public synchronized boolean runTask(Runnable newTask) {
		if (this.isAlive() || !started) {
			task = newTask;
			if (started) {
				this.notify();
			} else {
				this.start();
			}
			return true;
		} else {
			return false;
		}
	}
}
