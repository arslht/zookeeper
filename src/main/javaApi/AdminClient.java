package zk.lht.javaApi;

import java.io.IOException;
import java.util.Date;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class AdminClient implements Watcher {

	ZooKeeper zk;
	private String hostport;

	public AdminClient(String string) {
		// TODO Auto-generated constructor stub
		this.hostport = string;
	}

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub

	}
	
	private void start() throws IOException {
		// TODO Auto-generated method stub
		zk = new ZooKeeper(hostport, 15000, this);
	}	

	private void listState() throws KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		try {
			Stat stat = new Stat();
			byte masterData[] = zk.getData("/master", false, stat);
			Date startDate = new Date(stat.getCtime());
			System.out.println("Master: " + new String(masterData) + " since " + startDate);
		} catch (NoNodeException e) {
			System.out.println(e);
		}
		
		System.out.println("workers: ");
		for (String w:zk.getChildren("/workers", false)) {
			byte data[] = zk.getData("/workers/"+w, false, null);
			String state = new String(data);
			System.out.println("\t" + w + ": " + state);
		}
		
		System.out.println("tasks: ");
		for (String t:zk.getChildren("/assign", false)) {
			System.out.println("\t" + t);
		}
		
		
	}
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		AdminClient c = new AdminClient(args[0]);
		c.start();
		
		c.listState();
	}

}
