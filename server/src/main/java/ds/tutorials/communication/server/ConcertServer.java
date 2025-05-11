package ds.tutorials.communication.server;

import concert.ConcertService;
import ds.tutorials.synchronization.DistributedLock;
import ds.tutorials.synchronization.DistributedTx;
import ds.tutorials.synchronization.DistributedTxCoordinator;
import ds.tutorials.synchronization.DistributedTxParticipant;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import distributed.NameServiceClient;
import org.apache.zookeeper.KeeperException;

public class ConcertServer {
    private static final String SERVICE_NAME = "concert-service";
    private static final String PROTOCOL = "grpc";
    private int serverPort;
    private DistributedTx reservation;
    private byte[] leaderData;
    private AtomicBoolean isLeader = new AtomicBoolean(false);
    private DistributedLock leaderLock;
    ConcertCommandServiceImpl commandService;
    ConcertQueryServiceImpl queryService;
    String dataStore;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributedLock.setZooKeeperURL("localhost:2181");
        DistributedTx.setZooKeeperURL("localhost:2181"); // 2 face commit

         if (args.length < 3) {
            System.out.println("Usage: ConcertServer <port> <nameServiceAddress> <dataDir>");
            System.exit(1);
        }
        
        int port = Integer.parseInt(args[0]);
        String nameServiceAddress = args[1];
        String dataDir = args[2];

        // Register with name service
        NameServiceClient nameServiceClient = new NameServiceClient(nameServiceAddress);
        String hostAddress = "localhost"; // In production, this should be the actual host address
        nameServiceClient.registerService(SERVICE_NAME, hostAddress, port, PROTOCOL);

        
        // Add shutdown hook
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            System.out.println("Shutting down ConcertServer...");
//            server.shutdown();
//            try { leaderElection.close(); } catch (Exception ignore) {}
//        }));
        ConcertServer server = new ConcertServer(dataDir, port, nameServiceAddress);
        server.startServer();
    }

    public ConcertServer(String dataDir, int port, String nameServiceAddress) throws IOException, InterruptedException, KeeperException {
        this.serverPort = port;
        leaderLock = new DistributedLock("concert-lock");
        commandService = new ConcertCommandServiceImpl(nameServiceAddress, dataDir, this);
        queryService = new ConcertQueryServiceImpl(commandService.getConcerts());
        reservation = new DistributedTxParticipant(commandService);
    }

    public void startServer () throws IOException, InterruptedException, KeeperException {
        Server server = ServerBuilder.forPort(serverPort)
                .addService(commandService)
                .addService(queryService)
                .build();

        server.start();
        System.out.println("ConcertServer started, listening on port " + serverPort);

        tryToBeLeader();
        server.awaitTermination();
    }

    public boolean isLeader() { return isLeader.get(); }

    private synchronized void setCurrentLeaderData(byte[] leaderData) {
        this.leaderData = leaderData;
    }

    private void tryToBeLeader() throws KeeperException, InterruptedException {
        Thread leaderCampaignThread = new Thread(new
                LeaderCampaignThread());
        leaderCampaignThread.start();
    }

    // keep on trying to be the leader
    class LeaderCampaignThread implements Runnable {
        private byte[] currentLeaderData = null;
        @Override
        public void run() {
            System.out.println("Starting the leader Campaign");
            try {
                boolean leader = leaderLock.tryAcquireLock();
                while (!leader) {
                    byte[] leaderData =
                            leaderLock.getLockHolderData();
                    if (currentLeaderData != leaderData) {
                        currentLeaderData = leaderData;
                        setCurrentLeaderData(currentLeaderData);
                    }
                    Thread.sleep(10000);
                    leader = leaderLock.tryAcquireLock();
                }
                currentLeaderData = null;
                beTheLeader();
            } catch (Exception ignored){
            }
        }
    }

    public void setConcertData(String concertId, String tier, double price) {
        ConcertService.Concert.Builder concert = commandService.getConcerts().get(concertId);
        if (concert != null) {
            concert.putPrices(tier, price);
        }
    }

    public String getConcertData(String concertId) {
        ConcertService.Concert.Builder concert = commandService.getConcerts().get(concertId);
        if (concert != null ) {
            return dataStore.toString();
        }
        return null;
    }

    public synchronized String[] getCurrentLeaderData() {
        return new String(leaderData).split(":");
    }

    public List<String[]> getOthersData() throws KeeperException, InterruptedException {

        List<String[]> result = new ArrayList<>();
        List<byte[]> othersData = leaderLock.getOthersData();

        for (byte[] data : othersData) {
            String[] dataStrings = new String(data).split(":");
            result.add(dataStrings);
        }
        return result;
    }

    public DistributedTx getTransaction() {
        return reservation;
    }

    private void beTheLeader() {
        System.out.println("I got the leader lock. Now acting as primary");
        isLeader.set(true);
        reservation = new DistributedTxCoordinator(commandService);
    }
} 