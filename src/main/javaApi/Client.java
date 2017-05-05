package zk.lht.javaApi;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class Client implements Watcher {

	
	ZooKeeper zk;
	private String hostport;

	public Client(String hostport) {
		// TODO Auto-generated constructor stub
		this.hostport = hostport;
	}

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println(event);
	}

	private String queueCommand(String string) throws KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		while (true) {
			
			try {
				String name = zk.create("/tasks/task-", string.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
				return name;
			} catch (NodeExistsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ConnectionLossException e) {
				// 在/tasks下不存在以这个回话ID命名的节点时重试命令
			}
			
		}
		
	}

	private void start() throws IOException {
		// TODO Auto-generated method stub
		zk = new ZooKeeper(hostport, 15000, this);
	}
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		Client c = new Client(args[0]);
		
		c.start();
		
		String name = c.queueCommand(args[1]);
		System.out.println("Created " + name);
	}

}
