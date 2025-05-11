package ds.tutorials.synchronization;

public interface DistributedTxListener {
    void onGlobalCommit();
    void onGlobalAbort();
} 