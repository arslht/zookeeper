package zk.lht.javaApi;

import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker implements Watcher {

	private static final Logger log = LoggerFactory.getLogger(Worker.class);
	
	ZooKeeper zk;
	private static String hostport;
	private String status;
	Random random = new Random();
	String serverId = Integer.toHexString(random.nextInt());
	
	/**
	 * Constructor
	 * @param string
	 */
	public Worker(String string) {
		// TODO Auto-generated constructor stub
		this.hostport = string;
		//System.out.println(this.hostport);

	}

	/**
	 * watcher
	 */
	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		log.info(event.toString() + ", "+hostport);
	}

	
	/**
	 * 注册从节点
	 * 
	 */
	
	private void register() {
		// TODO Auto-generated method stub
		zk.create("/workers/worker-"+serverId, "Idle".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createWorkerCallback, null);
	}
	
	StringCallback createWorkerCallback = new StringCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			// TODO Auto-generated method stub
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				register();
				break;
			
			case OK:
				log.info("Register successfully: " + serverId);
				break;
			
			case NODEEXISTS:
				log.warn("Already created: " + serverId);
				break;
			
			default:
				log.error("Something went wrong: " + KeeperException.create(Code.get(rc), path));
				
			}
		}	
	};


	/**
	 * 设置从节点状态
	 * @param status
	 */

	private String name = "working";
	
	public void setStatus(String status) {
		this.status = status;
		updateStatus(status);
	}

	private void updateStatus(String status) {
		// TODO Auto-generated method stub
		if (status == this.status) {
			zk.setData("/workers/" + name, status.getBytes(), -1, statusUpdateCallback, status);
		}
	}
	
	StatCallback statusUpdateCallback = new StatCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			// TODO Auto-generated method stub
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				updateStatus((String)ctx);
				return;
			}
		}
		
	};

	/**
	 * 
	 * @throws IOException
	 */
	private void startZK() throws IOException {
		// TODO Auto-generated method stub
		zk = new ZooKeeper(hostport, 15000, this);
	}
	
	
	public static void main(String args[]) throws InterruptedException, IOException {
		// TODO Auto-generated method stub
		//String work1 = "127.0.0.1:2181";
		Worker w = new Worker(args[0]);
		
		w.startZK();
		w.register();
		
		Thread.sleep(60000);
	}


}
