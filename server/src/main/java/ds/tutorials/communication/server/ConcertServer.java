package ds.tutorials.communication.server;

import concert.ConcertService;
import ds.tutorials.synchronization.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import distributed.NameServiceClient;
import org.apache.zookeeper.KeeperException;
import org.json.JSONObject;

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

    public static void main(String[] args) throws Exception {
        DistributedLock.setZooKeeperURL("localhost:2181");
        DistributedTx.setZooKeeperURL("localhost:2181"); // 2 phase commit

         if (args.length < 3) {
            System.out.println("Usage: ConcertServer <port> <nameServiceAddress> <dataDir>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String nameServiceAddress = args[1];
        String dataDir = args[2];

        // Register with name service
        NameServiceClient nameServiceClient = new NameServiceClient(nameServiceAddress);
        String hostAddress = "localhost";
        nameServiceClient.registerService(SERVICE_NAME, hostAddress, port, PROTOCOL);

        ConcertServer server = new ConcertServer(dataDir, port, nameServiceAddress);
        server.startServer();
    }


    public ConcertServer(String dataDir, int port, String nameServiceAddress) throws IOException, InterruptedException, KeeperException {
        this.serverPort = port;
        // Build JSON info
        JSONObject nodeInfo = new JSONObject();
        String hostAddress = InetAddress.getLocalHost().getHostAddress();

        nodeInfo.put("ip", hostAddress);
        nodeInfo.put("port", serverPort);

        String json = nodeInfo.toString();
        String encoded = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
        byte[] encodedBytes = encoded.getBytes(StandardCharsets.UTF_8);

        leaderLock = new DistributedLock("concert-lock", encodedBytes);
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
        if (leaderData != null && leaderData.length > 0) {
            return new String(leaderData, StandardCharsets.UTF_8).split(":");
        }
        return new String[0];
    }

    public List<String[]> getOthersData() throws KeeperException, InterruptedException {
        List<String[]> result = new ArrayList<>();
        List<byte[]> othersData = leaderLock.getOthersData();

        for (byte[] data : othersData) {
            if (data == null || data.length == 0) {
                System.out.println("Skipping node with no data");
                continue;
            }
            try {
                String rawString = new String(data, StandardCharsets.UTF_8);
                System.out.println("Raw data from ZooKeeper: " + rawString);
                String jsonStr;
                try {
                    byte[] decodedBytes = Base64.getDecoder().decode(rawString);
                    jsonStr = new String(decodedBytes, StandardCharsets.UTF_8);
                    System.out.println("Decoded from Base64: " + jsonStr);
                } catch (IllegalArgumentException e) {
                    System.out.println("Not valid Base64, using raw string: " + e.getMessage());
                    jsonStr = rawString;
                }
                try {
                    JSONObject jsonObject = new JSONObject(jsonStr);

                    if (jsonObject.has("ip") && jsonObject.has("port")) {
                        String ip = jsonObject.getString("ip");
                        int port = jsonObject.getInt("port");
                        result.add(new String[]{ip, String.valueOf(port)});
                        System.out.println("Successfully parsed server data: IP=" + ip + ", Port=" + port);
                    } else {
                        System.err.println("JSON doesn't contain required fields: " + jsonStr);
                    }
                } catch (Exception e) {
                    System.err.println("Error parsing as JSON: " + e.getMessage());

                    if (jsonStr.contains(":")) {
                        String[] parts = jsonStr.split(":");
                        if (parts.length == 2) {
                            try {
                                String ip = parts[0].trim();
                                int port = Integer.parseInt(parts[1].trim());
                                result.add(new String[]{ip, String.valueOf(port)});
                                System.out.println("Parsed as ip:port format: " + ip + ":" + port);
                            } catch (NumberFormatException nfe) {
                                System.err.println("Invalid port in ip:port format: " + nfe.getMessage());
                            }
                        }
                    } else {
                        System.err.println("Unrecognized data format: " + jsonStr);
                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing node data: " + e.getMessage());
                e.printStackTrace();
            }
        }
        return result;
    }

        public DistributedTx getTransaction() {
        return reservation;
    }

    private void beTheLeader() {
        System.out.println("I got the leader lock. Now acting as primary");
        isLeader.set(true);

        try {
            // Create a JSON object with IP and port - use only the IP address without ZooKeeper port
            JSONObject leaderInfo = new JSONObject();
            leaderInfo.put("ip", "127.0.0.1");
            leaderInfo.put("port", serverPort);

            // Convert to string and Base64 encode
            String leaderJsonStr = leaderInfo.toString();
            String encodedData = Base64.getEncoder().encodeToString(leaderJsonStr.getBytes(StandardCharsets.UTF_8));

            // Store in ZooKeeper
            leaderLock.setLockNodeData(encodedData.getBytes(StandardCharsets.UTF_8));
            System.out.println("Leader data set: " + leaderJsonStr);
        } catch (Exception e) {
            System.err.println("Error setting leader data: " + e.getMessage());
            e.printStackTrace();
        }

        reservation = new DistributedTxCoordinator(commandService);
    }
} 