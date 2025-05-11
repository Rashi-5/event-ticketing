package ds.tutorials.synchronization;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class ZooKeeperClient {

    private ZooKeeper zooKeeper;

    public ZooKeeperClient(String zooKeeperUrl, int
            sessionTimeout, Watcher watcher) throws IOException {

        zooKeeper = new ZooKeeper(zooKeeperUrl, sessionTimeout, watcher);
    }

    public String createNode(String path, boolean shouldWatch, CreateMode mode, byte[] data)
            throws KeeperException, InterruptedException {
        String createdPath = zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
        return createdPath;
    }

    public String createNode(String path, boolean shouldWatch, CreateMode mode)
            throws KeeperException, InterruptedException, UnsupportedEncodingException {
        return createNode(path, shouldWatch, mode, "".getBytes("UTF-8"));
    }

    public boolean CheckExists(String path) throws
            KeeperException, InterruptedException {
        Stat nodeStat = zooKeeper.exists(path, false);
        return (nodeStat != null);
    }

    public void delete(String path) throws
            KeeperException, InterruptedException {
        zooKeeper.delete(path, -1);
    }

    public void forceDelete(String path) throws
            KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(path, false);
        for (String child : children) {
            forceDelete(path + "/" + child);
        }
        delete(path);
    }

    public List<String> getChildrenNodePaths(String root)
            throws KeeperException, InterruptedException {
        return zooKeeper.getChildren(root, false);
    }

    public void addWatch(String path) throws
            KeeperException, InterruptedException {
        zooKeeper.exists(path, true);
    }

    public byte[] getData(String path, boolean watch) throws
            KeeperException, InterruptedException {
        return zooKeeper.getData(path, watch, null);
    }

    public void write(String path, byte[] data) throws
            KeeperException, InterruptedException {
        zooKeeper.setData(path, data, -1);
    }
}
