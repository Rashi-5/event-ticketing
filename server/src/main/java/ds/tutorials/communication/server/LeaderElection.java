package ds.tutorials.communication.server;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    private static final String ELECTION_NAMESPACE = "/election";
    private final ZooKeeper zooKeeper;
    private String currentZnodeName;
    private boolean isLeader = false;
    private Runnable onElectedLeader;
    private Runnable onElectedFollower;

    public LeaderElection(String zkAddress) throws IOException, KeeperException, InterruptedException {
        this.zooKeeper = new ZooKeeper(zkAddress, 3000, this);
        ensureElectionZnode();
    }

    private void ensureElectionZnode() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(ELECTION_NAMESPACE, false);
        if (stat == null) {
            try {
                zooKeeper.create(ELECTION_NAMESPACE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ignore) {}
        }
    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/node_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void electLeader() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
        Collections.sort(children);
        String smallestChild = children.get(0);
        if (smallestChild.equals(currentZnodeName)) {
            isLeader = true;
            if (onElectedLeader != null) onElectedLeader.run();
        } else {
            isLeader = false;
            if (onElectedFollower != null) onElectedFollower.run();
            int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
            String watchNode = children.get(predecessorIndex);
            zooKeeper.exists(ELECTION_NAMESPACE + "/" + watchNode, this);
        }
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void setOnElectedLeader(Runnable callback) {
        this.onElectedLeader = callback;
    }

    public void setOnElectedFollower(Runnable callback) {
        this.onElectedFollower = callback;
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case NodeDeleted:
                try {
                    electLeader();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            default:
                break;
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }
} 