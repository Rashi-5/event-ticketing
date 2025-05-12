package ds.tutorials.synchronization;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributedLock implements Watcher {

    private String childPath;
    private ZooKeeperClient client;
    private String lockPath;
    private boolean isAcquired = false;
    private String watchedNode;
    CountDownLatch startFlag = new CountDownLatch(1);
    CountDownLatch eventReceivedFlag;
    public static String zooKeeperUrl ;
    private static String lockProcessPath = "/lp_";

    public static void setZooKeeperURL(String url){
        zooKeeperUrl = url;
    }

    public DistributedLock(String lockName, byte[] initData) throws IOException, KeeperException, InterruptedException {
        this.lockPath = "/" + lockName;
        client = new ZooKeeperClient(zooKeeperUrl, 5000, this);
        startFlag.await();
        if (client.CheckExists(lockPath) == false) {
            createRootNode();
        }
	    createChildNode(initData);
    }

    private void createRootNode() throws InterruptedException, UnsupportedEncodingException, KeeperException {
        lockPath = client.createNode(lockPath, false, CreateMode.PERSISTENT);
        System.out.println("Root node created at " + lockPath);
    }

    private void createChildNode(byte[] data)
            throws InterruptedException, UnsupportedEncodingException, KeeperException {
        childPath = client.createNode(lockPath + lockProcessPath, false, CreateMode.EPHEMERAL_SEQUENTIAL, data);
        System.out.println("Child node created at " + childPath);
    }


    public boolean tryAcquireLock() throws KeeperException, InterruptedException, UnsupportedEncodingException {
        String smallestNode = this.findSmallestNodePath();
        if (smallestNode.equals(this.childPath)) {
            this.isAcquired = true;
        }

        return this.isAcquired;
    }
    public byte[] getLockHolderData() throws KeeperException, InterruptedException {
        String smallestNode = this.findSmallestNodePath();
        return this.client.getData(smallestNode, true);
    }

    public List<byte[]> getOthersData() throws KeeperException, InterruptedException {
        List<byte[]> result = new ArrayList();
        List<String> childrenNodePaths = this.client.getChildrenNodePaths(this.lockPath);
        Iterator var3 = childrenNodePaths.iterator();

        while(var3.hasNext()) {
            String path = (String)var3.next();
            path = this.lockPath + "/" + path;
            if (!path.equals(this.childPath)) {
                byte[] data = this.client.getData(path, false);
                result.add(data);
            }
        }

        return result;
    }

    public void acquireLock() throws KeeperException, InterruptedException, UnsupportedEncodingException {
        String smallestNode = findSmallestNodePath();
        if (smallestNode.equals(childPath)) {
            isAcquired = true;
        } else {
            do {
                System.out.println("Lock is currently acquired by node " + smallestNode + " .. hence waiting..");
                eventReceivedFlag = new CountDownLatch(1);
                watchedNode = smallestNode;
                client.addWatch(smallestNode);
                eventReceivedFlag.await();
                smallestNode = findSmallestNodePath();
            } while (!smallestNode.equals(childPath));
            isAcquired = true;
        }
    }

    public void releaseLock() throws KeeperException, InterruptedException {
        if (!isAcquired) {
            throw new IllegalStateException("Lock needs to be acquired first to release");
        }
        client.delete(childPath);
        isAcquired = false;
    }

    private String findSmallestNodePath() throws KeeperException, InterruptedException {
        List<String> childrenNodePaths = null;
        childrenNodePaths = client.getChildrenNodePaths(lockPath);
        Collections.sort(childrenNodePaths);
        String smallestPath = childrenNodePaths.get(0);
        smallestPath = lockPath + "/" + smallestPath;
        return smallestPath;
    }

    public void setLockNodeData(byte[] data) throws KeeperException, InterruptedException {
        if (childPath != null) {
            client.setData(childPath, data);
            System.out.println("Set data on lock node: " + childPath);
        } else {
            throw new IllegalStateException("Child node path is null, cannot set data");
        }
    }
    @Override
    public void process(WatchedEvent event) {
        Event.KeeperState state = event.getState();
        Event.EventType type = event.getType();
        if (Event.KeeperState.SyncConnected == state) {
            if (Event.EventType.None == type) {
                // Identify successful connection
                System.out.println("Successful connected to the server");
                startFlag.countDown();
            }
        }
        if (Event.EventType.NodeDeleted.equals(type)){
            if (watchedNode != null && eventReceivedFlag
                    != null && event.getPath().equals(watchedNode)){
                System.out.println("NodeDelete event received. Trying to get the lock..");
                eventReceivedFlag.countDown();
            }
        }
    }
}
