package ds.tutorials.communication.server;

import concert.ConcertCommandServiceGrpc;
import concert.ConcertService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import ds.tutorials.synchronization.DistributedTxCoordinator;
import ds.tutorials.synchronization.DistributedTxListener;
import ds.tutorials.synchronization.DistributedLock;
import ds.tutorials.synchronization.DistributedTxParticipant;
import javafx.util.Pair;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.zookeeper.KeeperException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.nio.file.*;

public class ConcertCommandServiceImpl extends ConcertCommandServiceGrpc.ConcertCommandServiceImplBase implements DistributedTxListener {
    // In-memory data store for concerts and reservations
    private final Map<String, ConcertService.Concert.Builder> concerts = new ConcurrentHashMap<>();
    private final Map<String, ConcertService.ReservationResponse> reservations = new ConcurrentHashMap<>();
    private final DistributedTxCoordinator coordinator;
    private final String nodeId;
    private final String dataDir;
    private final String nameServiceAddress;
    private ConcertServer concertServer;
    private boolean transactionStatus = false;
    private Pair<String, Object> tempDataHolder;
    private ManagedChannel channel = null;
    private ConcertCommandServiceGrpc.ConcertCommandServiceBlockingStub clientStub = null;

    public ConcertCommandServiceImpl(String nameServiceAddress, String dataDir, ConcertServer concertServer) {
        this.nodeId = UUID.randomUUID().toString();
        this.coordinator = new DistributedTxCoordinator(this);
        this.dataDir = dataDir;
        this.nameServiceAddress = nameServiceAddress;
        this.concertServer = concertServer;
        loadData();
    }

    private void loadData() {
        try {
            Files.createDirectories(Paths.get(dataDir));
            loadConcerts();
            loadReservations();
        } catch (IOException e) {
            System.err.println("Failed to load data: " + e.getMessage());
        }
    }

    private void loadConcerts() throws IOException {
        File concertsFile = new File(dataDir, "concerts.dat");
        if (concertsFile.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(concertsFile))) {
                @SuppressWarnings("unchecked")
                Map<String, ConcertService.Concert> loadedConcerts = (Map<String, ConcertService.Concert>) ois.readObject();
                // Convert built objects back to builders
                for (Map.Entry<String, ConcertService.Concert> entry : loadedConcerts.entrySet()) {
                    concerts.put(entry.getKey(), entry.getValue().toBuilder());
                }
            } catch (ClassNotFoundException e) {
                System.err.println("Failed to load concerts: " + e.getMessage());
            }
        }
    }

    private void loadReservations() throws IOException {
        File reservationsFile = new File(dataDir, "reservations.dat");
        if (reservationsFile.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(reservationsFile))) {
                @SuppressWarnings("unchecked")
                Map<String, ConcertService.ReservationResponse> loadedReservations = (Map<String, ConcertService.ReservationResponse>) ois.readObject();
                reservations.putAll(loadedReservations);
            } catch (ClassNotFoundException e) {
                System.err.println("Failed to load reservations: " + e.getMessage());
            }
        }
    }

    private void saveData() {
        try {
            saveConcerts();
            saveReservations();
        } catch (IOException e) {
            System.err.println("Failed to save data: " + e.getMessage());
        }
    }

    private void saveConcerts() throws IOException {
        File concertsFile = new File(dataDir, "concerts.dat");
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(concertsFile))) {
            // Convert Builder objects to built objects before saving
            Map<String, ConcertService.Concert> concertsToSave = new HashMap<>();
            for (Map.Entry<String, ConcertService.Concert.Builder> entry : concerts.entrySet()) {
                concertsToSave.put(entry.getKey(), entry.getValue().build());
            }
            oos.writeObject(concertsToSave);
        }
    }

    private void saveReservations() throws IOException {
        File reservationsFile = new File(dataDir, "reservations.dat");
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(reservationsFile))) {
            oos.writeObject(new HashMap<>(reservations));
        }
    }

    private void startDistributedTx(String operationId, Object data) {
        try {
            concertServer.getTransaction().start(operationId, String.valueOf(UUID.randomUUID()));
            tempDataHolder = new Pair<>(operationId, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateSecondaryServers(String operationId, Object data) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers");
        List<String[]> othersData = concertServer.getOthersData();
        for (String[] serverData : othersData) {
            if (serverData.length < 2) {
                System.err.println("Invalid secondary server data: " + Arrays.toString(serverData));
                continue;
            }
            String IPAddress = serverData[0];
            int port = Integer.parseInt(serverData[1]);

            try {
                channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                        .usePlaintext()
                        .build();
                clientStub = ConcertCommandServiceGrpc.newBlockingStub(channel);

                // Create request based on operation type
                if (operationId.startsWith("add_concert_")) {
                    ConcertService.Concert concert = (ConcertService.Concert) data;
                    ConcertService.AddConcertRequest request = ConcertService.AddConcertRequest.newBuilder()
                            .setConcert(concert)
                            .setIsSentByPrimary(true)
                            .build();
                    clientStub.addConcert(request);
                } else if (operationId.startsWith("update_concert_")) {
                    ConcertService.Concert concert = (ConcertService.Concert) data;
                    ConcertService.UpdateConcertRequest request = ConcertService.UpdateConcertRequest.newBuilder()
                            .setConcert(concert)
                            .setIsSentByPrimary(true)
                            .build();
                    clientStub.updateConcert(request);
                } else if (operationId.startsWith("cancel_concert_")) {
                    String concertId = (String) data;
                    ConcertService.CancelConcertRequest request = ConcertService.CancelConcertRequest.newBuilder()
                            .setConcertId(concertId)
                            .setIsSentByPrimary(true)
                            .build();
                    clientStub.cancelConcert(request);
                } else if (operationId.startsWith("add_ticket_stock_")) {
                    ConcertService.AddTicketStockRequest originalRequest = (ConcertService.AddTicketStockRequest) data;
                    ConcertService.AddTicketStockRequest request = ConcertService.AddTicketStockRequest.newBuilder()
                            .setConcertId(originalRequest.getConcertId())
                            .setTier(originalRequest.getTier())
                            .setCount(originalRequest.getCount())
                            .setAfterParty(originalRequest.getAfterParty())
                            .setPrice(originalRequest.getPrice())
                            .setIsSentByPrimary(true)
                            .build();
                    clientStub.addTicketStock(request);
                } else if (operationId.startsWith("update_ticket_price_")) {
                    ConcertService.UpdateTicketPriceRequest originalRequest = (ConcertService.UpdateTicketPriceRequest) data;
                    ConcertService.UpdateTicketPriceRequest request = ConcertService.UpdateTicketPriceRequest.newBuilder()
                            .setConcertId(originalRequest.getConcertId())
                            .setTier(originalRequest.getTier())
                            .setPrice(originalRequest.getPrice())
                            .setIsSentByPrimary(true)
                            .build();
                    clientStub.updateTicketPrice(request);
                } else if (operationId.startsWith("reserve_tickets_")) {
                    ConcertService.ReserveTicketsRequest originalRequest = (ConcertService.ReserveTicketsRequest) data;
                    ConcertService.ReserveTicketsRequest request = ConcertService.ReserveTicketsRequest.newBuilder()
                            .setConcertId(originalRequest.getConcertId())
                            .setTier(originalRequest.getTier())
                            .setCount(originalRequest.getCount())
                            .setAfterParty(originalRequest.getAfterParty())
                            .setCustomerId(originalRequest.getCustomerId())
                            .setIsSentByPrimary(true)
                            .build();
                    clientStub.reserveTickets(request);
                } else if (operationId.startsWith("bulk_reserve_")) {
                    ConcertService.BulkReserveRequest originalRequest = (ConcertService.BulkReserveRequest) data;
                    ConcertService.BulkReserveRequest request = ConcertService.BulkReserveRequest.newBuilder()
                            .setConcertId(originalRequest.getConcertId())
                            .setTier(originalRequest.getTier())
                            .setCount(originalRequest.getCount())
                            .setAfterParty(originalRequest.getAfterParty())
                            .setGroupId(originalRequest.getGroupId())
                            .setIsSentByPrimary(true)
                            .build();
                    clientStub.bulkReserve(request);
                }
            } catch (Exception e) {
                System.err.println("Error communicating with secondary server " + IPAddress + ":" + port + ": " + e.getMessage());
            } finally {
                if (channel != null) {
                    channel.shutdown();
                }
            }
        }
    }

    private ConcertService.ConcertResponse callPrimary(ConcertService.AddConcertRequest request) {
        return callPrimaryServer(request, ConcertService.ConcertResponse.class);
    }

    private ConcertService.ConcertResponse callPrimary(ConcertService.UpdateConcertRequest request) {
        return callPrimaryServer(request, ConcertService.ConcertResponse.class);
    }

    private ConcertService.ConcertResponse callPrimary(ConcertService.ApplyDiscountRequest request) {
        return callPrimaryServer(request, ConcertService.ConcertResponse.class);
    }

    private ConcertService.ConcertResponse callPrimary(ConcertService.CancelConcertRequest request) {
        return callPrimaryServer(request, ConcertService.ConcertResponse.class);
    }

    private ConcertService.ConcertResponse callPrimary(ConcertService.AddTicketStockRequest request) {
        return callPrimaryServer(request, ConcertService.ConcertResponse.class);
    }

    private ConcertService.ConcertResponse callPrimary(ConcertService.UpdateTicketPriceRequest request) {
        return callPrimaryServer(request, ConcertService.ConcertResponse.class);
    }

    private ConcertService.ReservationResponse callPrimary(ConcertService.ReserveTicketsRequest request) {
        return callPrimaryServer(request, ConcertService.ReservationResponse.class);
    }

    private ConcertService.ReservationResponse callPrimary(ConcertService.BulkReserveRequest request) {
        return callPrimaryServer(request, ConcertService.ReservationResponse.class);
    }

    private <T> T callPrimaryServer(Object request, Class<T> responseType) {
        System.out.println("Calling Primary server");

        try {
            String[] currentLeaderData = concertServer.getCurrentLeaderData();

            // Check if leader data exists
            if (currentLeaderData == null || currentLeaderData.length == 0) {
                System.err.println("No leader data available");
                return getDefaultResponse(responseType);
            }

            System.out.println("Encoded leader data: " + currentLeaderData[0]);

            String encoded = currentLeaderData[0];
            try {
                byte[] decodedBytes = Base64.getDecoder().decode(encoded);
                String decoded = new String(decodedBytes, StandardCharsets.UTF_8);

                System.out.println("Decoded leader data: " + decoded);

                JSONObject json = new JSONObject(decoded);
                String IPAddress = json.getString("ip");
                int port = json.getInt("port");  // Use getInt() instead of getString()

                System.out.println("Connecting to leader at " + IPAddress + ":" + port);

                channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                        .usePlaintext()
                        .build();
                clientStub = ConcertCommandServiceGrpc.newBlockingStub(channel);

                if (request instanceof ConcertService.AddConcertRequest) {
                    return (T) clientStub.addConcert((ConcertService.AddConcertRequest) request);
                } else if (request instanceof ConcertService.UpdateConcertRequest) {
                    return (T) clientStub.updateConcert((ConcertService.UpdateConcertRequest) request);
                } else if (request instanceof ConcertService.CancelConcertRequest) {
                    return (T) clientStub.cancelConcert((ConcertService.CancelConcertRequest) request);
                } else if (request instanceof ConcertService.AddTicketStockRequest) {
                    return (T) clientStub.addTicketStock((ConcertService.AddTicketStockRequest) request);
                } else if (request instanceof ConcertService.UpdateTicketPriceRequest) {
                    return (T) clientStub.updateTicketPrice((ConcertService.UpdateTicketPriceRequest) request);
                } else if (request instanceof ConcertService.ReserveTicketsRequest) {
                    return (T) clientStub.reserveTickets((ConcertService.ReserveTicketsRequest) request);
                } else if (request instanceof ConcertService.BulkReserveRequest) {
                    return (T) clientStub.bulkReserve((ConcertService.BulkReserveRequest) request);
                }
            } catch (IllegalArgumentException e) {
                System.err.println("Error decoding Base64 data: " + e.getMessage());
                return getDefaultResponse(responseType);
            } catch (JSONException e) {
                System.err.println("Error parsing JSON: " + e.getMessage() + " from data: " + encoded);
                return getDefaultResponse(responseType);
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println("Leader data format is incorrect: " + e.getMessage());
            return getDefaultResponse(responseType);
        } catch (Exception e) {
            System.err.println("Error communicating with primary server: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (channel != null) {
                channel.shutdown();
            }
        }

        // Return a default response if communication fails
        return getDefaultResponse(responseType);
    }

    // Helper method to get default response
    private <T> T getDefaultResponse(Class<T> responseType) {
        if (responseType == ConcertService.ConcertResponse.class) {
            return (T) ConcertService.ConcertResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Failed to communicate with primary server")
                    .build();
        } else {
            return (T) ConcertService.ReservationResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Failed to communicate with primary server")
                    .build();
        }
    }

    @Override
    public void addConcert(ConcertService.AddConcertRequest request, StreamObserver<ConcertService.ConcertResponse> responseObserver) {
        System.out.println("[Command] Received addConcert request - Concert: " + request.getConcert().getName() +
                " (ID: " + request.getConcert().getId() + ", Date: " + request.getConcert().getDate() + ")");

        if (concertServer.isLeader()) {
            // Act as primary
            try {
                System.out.println("Adding concert as Primary");
                String operationId = "add_concert_" + request.getConcert().getId();
                startDistributedTx(operationId, request.getConcert());
                updateSecondaryServers(operationId, request.getConcert());
                ((DistributedTxCoordinator)concertServer.getTransaction()).perform();
                transactionStatus = true;  // Set to true after successful transaction

            } catch (Exception e) {
                System.out.println("Error while adding concert: " + e.getMessage());
                e.printStackTrace();
                transactionStatus = false;
            }
        } else {
            // Act as secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Adding concert on secondary, on Primary's command");
                String operationId = "add_concert_" + request.getConcert().getId();
                startDistributedTx(operationId, request.getConcert());
                ((DistributedTxParticipant)concertServer.getTransaction()).voteCommit();
                transactionStatus = true;  // Set to true after successful commit
            } else {
                // Forward to primary
                ConcertService.ConcertResponse response = callPrimary(request);
                transactionStatus = response.getSuccess();
            }
        }

        ConcertService.ConcertResponse response = ConcertService.ConcertResponse.newBuilder()
                .setSuccess(transactionStatus)
                .setMessage(transactionStatus ? "Concert added successfully." : "Failed to add concert.")
                .setConcert(request.getConcert())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void updateConcert(ConcertService.UpdateConcertRequest request, StreamObserver<ConcertService.ConcertResponse> responseObserver) {
        System.out.println("[Command] Received updateConcert request - Concert ID: " + request.getConcert().getId() +
                ", New Name: " + request.getConcert().getName() + ", New Date: " + request.getConcert().getDate());

        ConcertService.ConcertResponse response;

        try {
            if (concertServer.isLeader()) {
                System.out.println("Updating concert as Primary");
                String operationId = "update_concert_" + request.getConcert().getId();

                // Begin distributed transaction
                startDistributedTx(operationId, request.getConcert());

                // Inform secondaries
                updateSecondaryServers(operationId, request.getConcert());

                // Commit transaction
                ((DistributedTxCoordinator) concertServer.getTransaction()).perform();

                // Update in-memory store after commit
                concerts.put(request.getConcert().getId(), request.getConcert().toBuilder());

                response = ConcertService.ConcertResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Concert updated successfully.")
                        .setConcert(request.getConcert())
                        .build();

            } else {
                if (request.getIsSentByPrimary()) {
                    System.out.println("Updating concert on secondary, on Primary's command");
                    String operationId = "update_concert_" + request.getConcert().getId();
                    startDistributedTx(operationId, request.getConcert());
                    ((DistributedTxParticipant) concertServer.getTransaction()).voteCommit();

                    // Update local store after successful commit
                    concerts.put(request.getConcert().getId(), request.getConcert().toBuilder());

                    response = ConcertService.ConcertResponse.newBuilder()
                            .setSuccess(true)
                            .setMessage("Concert updated on secondary.")
                            .setConcert(request.getConcert())
                            .build();
                } else {
                    System.out.println("Not leader, forwarding update request to primary.");
                    response = callPrimary(request);  // forward and reuse response
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            response = ConcertService.ConcertResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Error during update: " + e.getMessage())
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void cancelConcert(ConcertService.CancelConcertRequest request, StreamObserver<ConcertService.ConcertResponse> responseObserver) {
        System.out.println("[Command] Received cancelConcert request - Concert ID: " + request.getConcertId());

        ConcertService.ConcertResponse response;

        try {
            String concertId = request.getConcertId();

            if (concertServer.isLeader()) {
                System.out.println("Cancelling concert as Primary");
                String operationId = "cancel_concert_" + concertId;

                startDistributedTx(operationId, concertId);
                updateSecondaryServers(operationId, concertId);
                ((DistributedTxCoordinator) concertServer.getTransaction()).perform();

                concerts.remove(concertId);

                response = ConcertService.ConcertResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Concert cancelled successfully.")
                        .build();

            } else {
                if (request.getIsSentByPrimary()) {
                    System.out.println("Cancelling concert on secondary, on Primary's command");
                    String operationId = "cancel_concert_" + concertId;

                    startDistributedTx(operationId, concertId);
                    ((DistributedTxParticipant) concertServer.getTransaction()).voteCommit();

                    concerts.remove(concertId);

                    response = ConcertService.ConcertResponse.newBuilder()
                            .setSuccess(true)
                            .setMessage("Concert cancelled on secondary.")
                            .build();
                } else {
                    System.out.println("Not leader, forwarding cancel request to primary.");
                    response = callPrimary(request);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            response = ConcertService.ConcertResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Error during cancellation: " + e.getMessage())
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void addTicketStock(ConcertService.AddTicketStockRequest request, StreamObserver<ConcertService.ConcertResponse> responseObserver) {
        System.out.println("[Command] Received addTicketStock request - Concert ID: " + request.getConcertId() +
                ", Tier: " + request.getTier() + ", Count: " + request.getCount() + ", After Party: " + request.getAfterParty());

        ConcertService.ConcertResponse response;
        String concertId = request.getConcertId();

        try {
            if (concertServer.isLeader()) {
                System.out.println("Updating ticket stock as Primary");

                String operationId = "add_ticket_stock_" + concertId + "_" + request.getTier();
                startDistributedTx(operationId, request);
                updateSecondaryServers(operationId, request);
                ((DistributedTxCoordinator) concertServer.getTransaction()).perform();

                applyTicketStockUpdate(request); // This modifies the local concert state

                ConcertService.Concert.Builder concert = concerts.get(concertId);

                response = ConcertService.ConcertResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Ticket stock updated.")
                        .setConcert(concert.build())
                        .build();

            } else {
                if (request.getIsSentByPrimary()) {
                    System.out.println("Secondary updating ticket stock on Primary's command");

                    String operationId = "add_ticket_stock_" + concertId + "_" + request.getTier();
                    startDistributedTx(operationId, request);
                    ((DistributedTxParticipant) concertServer.getTransaction()).voteCommit();

                    applyTicketStockUpdate(request); // Modify local copy

                    ConcertService.Concert.Builder concert = concerts.get(concertId);

                    response = ConcertService.ConcertResponse.newBuilder()
                            .setSuccess(true)
                            .setMessage("Ticket stock updated on secondary.")
                            .setConcert(concert.build())
                            .build();
                } else {
                    System.out.println("Forwarding ticket stock update to primary.");
                    response = callPrimary(request);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            response = ConcertService.ConcertResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Error updating ticket stock: " + e.getMessage())
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void applyTicketStockUpdate(ConcertService.AddTicketStockRequest request) {
        String concertId = request.getConcertId();
        ConcertService.Concert.Builder concert = concerts.get(concertId);
        if (concert == null) return;

        // Calculate current total ticket count (seat tiers + after-party)
        int currentSeatTotal = concert.getSeatTiersMap().values().stream().mapToInt(Integer::intValue).sum();
        int currentAfterParty = concert.getAfterPartyTickets();
        int currentTotal = currentSeatTotal + currentAfterParty;

        // Get max allowed
        int maxAllowed = concert.getMaxTicketCount();
        int newTickets = request.getCount();

        // Check if this update would exceed the max
        if (currentTotal + newTickets > maxAllowed) {
            System.err.println("Cannot add tickets: would exceed maxTicketCount (" + maxAllowed + ")");
            return;
        }

        if (request.getAfterParty()) {
            concert.setAfterPartyTickets(currentAfterParty + newTickets);
        } else {
            Map<String, Integer> seatTiers = new HashMap<>(concert.getSeatTiersMap());
            seatTiers.put(request.getTier(), seatTiers.getOrDefault(request.getTier(), 0) + newTickets);
            concert.putAllSeatTiers(seatTiers);

            if (request.getPrice() > 0) {
                Map<String, Double> prices = new HashMap<>(concert.getPricesMap());
                prices.put(request.getTier(), request.getPrice());
                concert.putAllPrices(prices);
            }
        }

        concerts.put(concertId, concert);
    }

    @Override
    public void updateTicketPrice(ConcertService.UpdateTicketPriceRequest request, StreamObserver<ConcertService.ConcertResponse> responseObserver) {
        System.out.println("[Command] Received updateTicketPrice request - Concert ID: " + request.getConcertId() +
                ", Tier: " + request.getTier() + ", Price: " + request.getPrice());

        ConcertService.ConcertResponse response;
        String concertId = request.getConcertId();

        try {
            if (concertServer.isLeader()) {
                System.out.println("Updating ticket price as Primary");

                String operationId = "update_ticket_price_" + concertId + "_" + request.getTier();
                startDistributedTx(operationId, request);
                updateSecondaryServers(operationId, request);
                ((DistributedTxCoordinator) concertServer.getTransaction()).perform();

                applyTicketPriceUpdate(request);

                ConcertService.Concert.Builder concert = concerts.get(concertId);
                response = ConcertService.ConcertResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Ticket price updated.")
                        .setConcert(concert.build())
                        .build();

            } else {
                if (request.getIsSentByPrimary()) {
                    System.out.println("Secondary updating ticket price on Primary's command");

                    String operationId = "update_ticket_price_" + concertId + "_" + request.getTier();
                    startDistributedTx(operationId, request);
                    ((DistributedTxParticipant) concertServer.getTransaction()).voteCommit();

                    applyTicketPriceUpdate(request);

                    ConcertService.Concert.Builder concert = concerts.get(concertId);
                    response = ConcertService.ConcertResponse.newBuilder()
                            .setSuccess(true)
                            .setMessage("Ticket price updated on secondary.")
                            .setConcert(concert.build())
                            .build();
                } else {
                    System.out.println("Forwarding ticket price update to primary.");
                    response = callPrimary(request);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            response = ConcertService.ConcertResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Error updating ticket price: " + e.getMessage())
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
    private void applyTicketPriceUpdate(ConcertService.UpdateTicketPriceRequest request) {
        String concertId = request.getConcertId();
        ConcertService.Concert.Builder concert = concerts.get(concertId);
        if (concert == null) return;

        Map<String, Double> prices = new HashMap<>(concert.getPricesMap());
        prices.put(request.getTier(), request.getPrice());
        concert.putAllPrices(prices);

        concertServer.setConcertData(concertId, request.getTier(), request.getPrice());
        concerts.put(concertId, concert);
        saveData();
    }

    @Override
    public void reserveTickets(ConcertService.ReserveTicketsRequest request, StreamObserver<ConcertService.ReservationResponse> responseObserver) {
        System.out.println("[Command] Received reserveTickets request - Concert ID: " + request.getConcertId() +
                ", Tier: " + request.getTier() + ", Count: " + request.getCount() +
                ", After Party: " + request.getAfterParty() + ", Customer ID: " + request.getCustomerId());

        boolean transactionStatus = false;
        String reservationId = UUID.randomUUID().toString();

        try {
            if (concertServer.isLeader()) {
                // PRIMARY NODE logic
                System.out.println("Reserving tickets as Primary");
                String operationId = "reserve_tickets_" + UUID.randomUUID();
                startDistributedTx(operationId, request);  // stores the request internally

                updateSecondaryServers(operationId, request);  // tell secondaries to prepare

                // Perform the reservation (locally on primary)
                transactionStatus = performReservation(request);

                // Trigger commit/abort to all nodes via 2PCf
                ((DistributedTxCoordinator) concertServer.getTransaction()).perform();

            } else {
                if (request.getIsSentByPrimary()) {
                    // SECONDARY NODE logic (executing command from primary)
                    System.out.println("Reserving tickets on secondary, on Primary's command");
                    String operationId = "reserve_tickets_" + UUID.randomUUID();
                    startDistributedTx(operationId, request);

                    transactionStatus = performReservation(request);
                    ((DistributedTxParticipant) concertServer.getTransaction()).voteCommit();

                } else {
                    // FORWARD to primary
                    ConcertService.ReservationResponse response = callPrimary(request);
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    return;
                }
            }
        } catch (Exception e) {
            System.err.println("Error during reservation: " + e.getMessage());
            e.printStackTrace();
        }

        // Build response for client (leader or secondary)
        ConcertService.ReservationResponse response = ConcertService.ReservationResponse.newBuilder()
                .setSuccess(transactionStatus)
                .setMessage(transactionStatus ? "Tickets reserved successfully." : "Failed to reserve tickets.")
                .setReservationId(reservationId)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private boolean performReservation(ConcertService.ReserveTicketsRequest request) {
        String concertId = request.getConcertId();
        ConcertService.Concert.Builder concert = concerts.get(concertId);

        if (concert == null) {
            System.err.println("Concert not found.");
            return false;
        }

        synchronized (concert) {
            int availableSeats = concert.getSeatTiersMap().getOrDefault(request.getTier(), 0);
            int availableAfterParty = concert.getAfterPartyTickets();

            if (request.getCount() > availableSeats) {
                System.err.println("Not enough seats.");
                return false;
            }

            if (request.getAfterParty() && request.getCount() > availableAfterParty) {
                System.err.println("Not enough after-party tickets.");
                return false;
            }

            // Deduct seats
            Map<String, Integer> updatedTiers = new HashMap<>(concert.getSeatTiersMap());
            updatedTiers.put(request.getTier(), availableSeats - request.getCount());
            concert.putAllSeatTiers(updatedTiers);

            // Deduct after-party
            if (request.getAfterParty()) {
                concert.setAfterPartyTickets(availableAfterParty - request.getCount());
            }

            // Update concert map
            getConcerts().put(concertId, concert);
            concerts.put(concertId, concert);

            // Save to disk
            saveData();
            return true;
        }
    }

    @Override
    public void bulkReserve(ConcertService.BulkReserveRequest request, StreamObserver<ConcertService.ReservationResponse> responseObserver) {
        System.out.println("[Command] Received bulkReserve request - Concert ID: " + request.getConcertId() +
                ", Tier: " + request.getTier() + ", Count: " + request.getCount() +
                ", After Party: " + request.getAfterParty() + ", Group ID: " + request.getGroupId());

        ConcertService.ReservationResponse response;

        try {
            String concertId = request.getConcertId();

            if (concertServer.isLeader()) {
                System.out.println("Processing bulkReserve as Primary");

                String operationId = "bulk_reserve_" + concertId + "_" + request.getGroupId();
                startDistributedTx(operationId, request);
                updateSecondaryServers(operationId, request);
                ((DistributedTxCoordinator) concertServer.getTransaction()).perform();

                response = applyBulkReservation(request);

            } else {
                if (request.getIsSentByPrimary()) {
                    System.out.println("Secondary processing bulkReserve on Primary's command");

                    String operationId = "bulk_reserve_" + concertId + "_" + request.getGroupId();
                    startDistributedTx(operationId, request);
                    ((DistributedTxParticipant) concertServer.getTransaction()).voteCommit();

                    response = applyBulkReservation(request);
                } else {
                    System.out.println("Forwarding bulkReserve request to primary.");
                    response = callPrimary(request);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            response = ConcertService.ReservationResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Error during bulk reservation: " + e.getMessage())
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
    private ConcertService.ReservationResponse applyBulkReservation(ConcertService.BulkReserveRequest request) {
        String concertId = request.getConcertId();
        ConcertService.Concert.Builder concert = concerts.get(concertId);

        if (concert == null) {
            return ConcertService.ReservationResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Concert not found.")
                    .build();
        }

        synchronized (concert) {
            int availableSeats = concert.getSeatTiersMap().getOrDefault(request.getTier(), 0);
            int availableAfterParty = concert.getAfterPartyTickets();

            if (request.getCount() > availableSeats) {
                return ConcertService.ReservationResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Not enough seats available.")
                        .build();
            }

            if (request.getAfterParty() && request.getCount() > availableAfterParty) {
                return ConcertService.ReservationResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Not enough after-party tickets available.")
                        .build();
            }

            // Update seats and after-party tickets
            Map<String, Integer> seatTiers = new HashMap<>(concert.getSeatTiersMap());
            seatTiers.put(request.getTier(), availableSeats - request.getCount());
            concert.putAllSeatTiers(seatTiers);

            if (request.getAfterParty()) {
                concert.setAfterPartyTickets(availableAfterParty - request.getCount());
            }

            concerts.put(concertId, concert);

            // Generate and save reservation
            String reservationId = UUID.randomUUID().toString();
            ConcertService.ReservationResponse reservation = ConcertService.ReservationResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Bulk reservation successful.")
                    .setReservationId(reservationId)
                    .build();

            reservations.put(reservationId, reservation);
            saveData();

            return reservation;
        }
    }

    @Override
    public void applyDiscount(ConcertService.ApplyDiscountRequest request, StreamObserver<ConcertService.ConcertResponse> responseObserver) {
        System.out.println("[Command] Received applyDiscount request - Concert ID: " + request.getConcertId() +
                ", Tier: " + request.getTier() + ", Discount: " + request.getDiscountPercentage() + "%");

        boolean transactionStatus = false;
        ConcertService.ConcertResponse response;

        if (concertServer.isLeader()) {
            try {
                String operationId = "apply_discount_" + UUID.randomUUID();
                startDistributedTx(operationId, request);
                updateSecondaryServers(operationId, request);

                transactionStatus = applyDiscountInternal(request); // apply discount before 2PC commit
                ((DistributedTxCoordinator) concertServer.getTransaction()).perform();

                if (!transactionStatus) {
                    response = ConcertService.ConcertResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage("Concert or tier not found.")
                            .build();
                } else {
                    response = ConcertService.ConcertResponse.newBuilder()
                            .setSuccess(true)
                            .setMessage("Discount applied successfully.")
                            .setConcert(concerts.get(request.getConcertId()).build())
                            .build();
                }

            } catch (Exception e) {
                e.printStackTrace();
                response = ConcertService.ConcertResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Error during discount: " + e.getMessage())
                        .build();
            }

        } else {
            if (request.getIsSentByPrimary()) {
                try {
                    String operationId = "apply_discount_" + UUID.randomUUID();
                    startDistributedTx(operationId, request);
                    transactionStatus = applyDiscountInternal(request); // must apply discount here
                    ((DistributedTxParticipant) concertServer.getTransaction()).voteCommit();

                    response = ConcertService.ConcertResponse.newBuilder()
                            .setSuccess(transactionStatus)
                            .setMessage(transactionStatus ? "Discount applied on secondary." : "Tier or concert not found.")
                            .build();

                } catch (Exception e) {
                    e.printStackTrace();
                    response = ConcertService.ConcertResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage("Error on secondary: " + e.getMessage())
                            .build();
                }
            } else {
                ConcertService.ConcertResponse primaryResponse = callPrimary(request);
                responseObserver.onNext(primaryResponse);
                responseObserver.onCompleted();
                return;
            }
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private boolean applyDiscountInternal(ConcertService.ApplyDiscountRequest request) {
        String concertId = request.getConcertId();
        ConcertService.Concert.Builder concert = concerts.get(concertId);
        if (concert == null) {
            return false;
        }

        Map<String, Double> prices = new HashMap<>(concert.getPricesMap());
        double discount = request.getDiscountPercentage();

        if (request.getTier().isEmpty()) {
            for (Map.Entry<String, Double> entry : prices.entrySet()) {
                double newPrice = entry.getValue() * (1 - discount / 100.0);
                prices.put(entry.getKey(), newPrice);
            }
        } else {
            String tier = request.getTier();
            if (!prices.containsKey(tier)) {
                return false;
            }
            double newPrice = prices.get(tier) * (1 - discount / 100.0);
            prices.put(tier, newPrice);
        }

        concert.putAllPrices(prices);
        concerts.put(concertId, concert);
        saveData();
        return true;
    }


    // Expose concerts map for query service
    public Map<String, ConcertService.Concert.Builder> getConcerts() {
        return concerts;
    }

    @Override
    public void onGlobalCommit() {
        if (tempDataHolder != null) {
            String operationId = tempDataHolder.getKey();
            Object data = tempDataHolder.getValue();

            // Handle the committed data based on operation type
            if (operationId.startsWith("add_concert_")) {
                ConcertService.Concert concert = (ConcertService.Concert) data;
                concerts.put(concert.getId(), concert.toBuilder());
            }

            tempDataHolder = null;
            saveData();
        }
    }

    @Override
    public void onGlobalAbort() {
        tempDataHolder = null;
        System.out.println("Transaction Aborted by the Coordinator");
    }
}
