package uk.co.connorhd.util.concurrent;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

public class TaskThreadPool {
	private int maxThreads;
	private int minThreads;
	private int keepIdleThreads;
	protected int keepAlive;

	private Queue<Runnable> taskQueue = new LinkedBlockingQueue<Runnable>();
	private int threadCount = 0;
	private Set<TaskThread> idleThreads = new HashSet<TaskThread>();
	private boolean hasShutdown = false;

	/**
	 * Create a TaskThreadPool
	 * 
	 * @param maxThreads
	 *            This is the maximum possible number of concurrent threads
	 * @param minThreads
	 *            This number of threads will always exist in the pool, even if
	 *            no tasks have been run
	 * @param keepIdleThreads
	 *            Threads will not end if less than this number of idle threads
	 *            are available, can not be greater than minThreads
	 * @param keepAlive
	 *            Threads will not end before this timeout after their last task
	 *            ends
	 */
	public TaskThreadPool(int maxThreads, int minThreads, int keepIdleThreads, int keepAlive) {
		this.maxThreads = maxThreads;
		this.minThreads = minThreads;
		this.keepIdleThreads = Math.min(minThreads, keepIdleThreads);
		this.keepAlive = keepAlive;

		for (int i = 0; i < minThreads; i++) {
			TaskThread thread = new TaskThread(this);
			thread.start();
			threadCount++;
			idleThreads.add(thread);
		}
	}

	/**
	 * Add a task to the pool, task will be run immediately if an idle thread is
	 * available or if the pool has capacity for a new thread, otherwise it will
	 * be queued.
	 * 
	 * @param task
	 *            Runnable to be run on the thread pool
	 */
	synchronized public void addTask(Runnable task) {
		if (hasShutdown) {
			throw new TaskThreadPoolShutdownException();
		}

		for (Iterator<TaskThread> i = idleThreads.iterator(); i.hasNext();) {
			TaskThread thread = i.next();
			if (thread.runTask(task)) {
				i.remove();
				return;
			}
		}

		if (threadCount < maxThreads) {
			TaskThread thread = new TaskThread(this);
			threadCount++;
			thread.runTask(task);
			return;
		}

		taskQueue.add(task);
	}

	/**
	 * Shutdown the thread pool, any running tasks will be allowed to complete,
	 * adding a task after calling this will cause a
	 * {@link TaskThreadPoolShutdownException}
	 */
	synchronized public void shutdown() {
		hasShutdown = true;
		for (TaskThread thread : idleThreads) {
			thread.interrupt();
		}
	}

	public boolean hasShutdown() {
		return hasShutdown;
	}

	synchronized protected Runnable getTaskForThread(TaskThread thread) {
		Runnable task = taskQueue.poll();
		if (task == null) {
			idleThreads.add(thread);
			return null;
		} else {
			return task;
		}
	}

	synchronized protected boolean threadWillEnd(TaskThread thread) {
		if (idleThreads.size() > keepIdleThreads && threadCount > minThreads) {
			idleThreads.remove(thread);
			threadCount--;
			return true;
		} else {
			return false;
		}
	}
}
