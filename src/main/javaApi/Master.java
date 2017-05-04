/**
 * 同步Master
 * @author lht
 */

import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Master implements Watcher {

	ZooKeeper zk;
	String hostport;
	Random random = new Random();
	
	String serverId = Integer.toHexString(random.nextInt());
	boolean isLeader = false;
	 
	
	boolean checkMaster() throws KeeperException, InterruptedException {
		while(true) {
			try {
				Stat stat = new Stat();
				byte data[] = zk.getData("/master", false, stat);
				isLeader = new String(data).equals(serverId);
				return true;
			}
			catch(NoNodeException e) {
				return false;
			}
			catch(ConnectionLossException e) {
				
			}
		}
	}
	
	void runForMaster() throws InterruptedException, KeeperException {
		while (true) {
			try {
				zk.create("/master", serverId.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				isLeader =true;
				break;
			} catch (NodeExistsException e) {
				isLeader = false;
				break;
			} catch (ConnectionLossException e) {
				
			}
			if (checkMaster()) break;
			
		} 
		
	}
	
	Master(String hostport) {
		this.hostport = hostport;
	}
	
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
		Master m = new Master(args[0]);
		m.startZK();
		m.runForMaster();
		
		if(m.isLeader) {
			System.out.println("I'm the leader");
			Thread.sleep(600000);
		}else{
			System.out.println("Someone else is the leader");
		}
		
		m.stopZK();
	}

}
