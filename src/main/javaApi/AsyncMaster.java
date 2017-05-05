package zk.lht.javaApi;

import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class AsyncMaster implements Watcher {

	private static final Logger LOG = LoggerFactory.getLogger(AsyncMaster.class);
	
	ZooKeeper zk;
	String hostport;
	Random random = new Random();

	String serverId = Integer.toHexString(random.nextInt());
	static boolean isLeader = false;

	/*
	 * 设置元数据
	 */
	public void bootstrap() {
		createParent("/workers", new byte[0]);
		createParent("/assign", new byte[0]);
		createParent("/tasks", new byte[0]);
		createParent("/status", new byte[0]);
	}
	
	void createParent(String path, byte[] data) {
		// TODO Auto-generated method stub
		zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
	}
	
	StringCallback createParentCallback = new StringCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			// TODO Auto-generated method stub
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				createParent(path, (byte[]) ctx);
				LOG.info("try to create " + path);
			case OK:
				LOG.info(path+"Parent created");
				break;
			case NODEEXISTS:
				LOG.warn("Parent already registered: " + path);
				break;
			default:
				LOG.error("something went wrong: " + KeeperException.create(Code.get(rc)),path);
			}
		}
		
	};

	/*
	 * 异步获取管理权
	 */
	StringCallback masterCreateCallback = new StringCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			// TODO Auto-generated method stub
			
			switch(Code.get(rc)){
			case CONNECTIONLOSS:
				LOG.info("LOSS");
				runForMaster();
				return;
			case OK:
				isLeader = true;
				try {
					System.out.println(zk.getData(path, false, null));
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				LOG.info("rfm OK");
				break;
			default:
				isLeader = false;
				LOG.error("rc = " + Code.get(rc));
			}
			System.out.println("I'm "+ (isLeader ? "" : "not") + "the leader");
		}
		
	};
	
	void runForMaster() {
		zk.create("/master", serverId.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
		
	}
	
	/*
	 * checkMaster方法：通过回调方法实现处理逻辑
	 * getData完成后，后续在DataCallback对象中继续
	 */
	
	DataCallback masterCheckCallback = new DataCallback() {
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkMaster();
				
				break;
			case NONODE:
				runForMaster();
				
				break;
				
			case OK:
				if (new String(data).equals(serverId)) {
					isLeader = true;
					System.out.println("check ok");
				}
				else {
					checkMaster();
				break;
				}
			default:
				LOG.error("code is " + Code.get(rc));
				
				
			}
		}
	};
	
	void checkMaster() {
		// TODO Auto-generated method stub
		zk.getData("/master", false, masterCheckCallback, null);
	}
	
	/*
	 * Constructor
	 */
	
	AsyncMaster(String hostport) {
		this.hostport = hostport;
	}

	/*
	 *  启动
	 */
	
	void startZK() throws IOException {
		zk = new ZooKeeper(hostport, 15000, this);	
	}
	
	void stopZK() throws InterruptedException {
		zk.close();
	}
	
	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println(event);
	}
	
	
	
	public static void main(String args[]) throws IOException, InterruptedException, KeeperException {
		AsyncMaster m = new AsyncMaster("127.0.0.1:2181");
		
		m.startZK();
		m.runForMaster();
		m.bootstrap();
		
		if(isLeader) {
			System.out.println("I'm the leader");
			//m.bootstrap();
			Thread.sleep(600000);
			//m.stopZK();
		}else{
			System.out.println("someone else is the leader");
		}
		Thread.sleep(600000);
		m.stopZK();
	}

}
