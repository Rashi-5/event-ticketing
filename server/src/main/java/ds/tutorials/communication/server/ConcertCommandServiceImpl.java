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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.zookeeper.KeeperException;
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

                // Create appropriate request based on operation type
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
        String[] currentLeaderData = concertServer.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);

        try {
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
        } catch (Exception e) {
            System.err.println("Error communicating with primary server: " + e.getMessage());
        } finally {
            if (channel != null) {
                channel.shutdown();
            }
        }

        // Return a default response if communication fails
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
    public void onGlobalCommit() {
        if (tempDataHolder != null) {
            String operationId = tempDataHolder.getKey();
            Object data = tempDataHolder.getValue();
            
            // Handle the committed data based on operation type
            if (operationId.startsWith("add_concert_")) {
                ConcertService.Concert concert = (ConcertService.Concert) data;
                concerts.put(concert.getId(), concert.toBuilder());
            }
            // Add other operation types here
            
            tempDataHolder = null;
            saveData();
        }
    }

    @Override
    public void onGlobalAbort() {
        tempDataHolder = null;
        System.out.println("Transaction Aborted by the Coordinator");
    }

    @Override
    public void updateConcert(ConcertService.UpdateConcertRequest request, StreamObserver<ConcertService.ConcertResponse> responseObserver) {
        System.out.println("[Command] Received updateConcert request - Concert ID: " + request.getConcert().getId() + 
            ", New Name: " + request.getConcert().getName() + ", New Date: " + request.getConcert().getDate());
        try {
//            concertLock.acquireLock();
            try {
                ConcertService.Concert concert = request.getConcert();
                concerts.put(concert.getId(), concert.toBuilder());
                ConcertService.ConcertResponse response = ConcertService.ConcertResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Concert updated successfully.")
                        .setConcert(concert)
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } finally {
//                concertLock.releaseLock();
            }
        } catch (Exception e) {
            ConcertService.ConcertResponse response = ConcertService.ConcertResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Distributed lock error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void cancelConcert(ConcertService.CancelConcertRequest request, StreamObserver<ConcertService.ConcertResponse> responseObserver) {
        System.out.println("[Command] Received cancelConcert request - Concert ID: " + request.getConcertId());
        try {
//            concertLock.acquireLock();
            try {
                String concertId = request.getConcertId();
                concerts.remove(concertId);
                ConcertService.ConcertResponse response = ConcertService.ConcertResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Concert cancelled successfully.")
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } finally {
//                concertLock.releaseLock();
            }
        } catch (Exception e) {
            ConcertService.ConcertResponse response = ConcertService.ConcertResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Distributed lock error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void addTicketStock(ConcertService.AddTicketStockRequest request, StreamObserver<ConcertService.ConcertResponse> responseObserver) {
        System.out.println("[Command] Received addTicketStock request - Concert ID: " + request.getConcertId() + 
            ", Tier: " + request.getTier() + ", Count: " + request.getCount() + 
            ", After Party: " + request.getAfterParty());
        try {
//            concertLock.acquireLock();
            try {
                String concertId = request.getConcertId();
                ConcertService.Concert.Builder concert = concerts.get(concertId);
                if (concert == null) {
                    ConcertService.ConcertResponse response = ConcertService.ConcertResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage("Concert not found.")
                            .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    return;
                }
                if (request.getAfterParty()) {
                    int current = concert.getAfterPartyTickets();
                    concert.setAfterPartyTickets(current + request.getCount());
                } else {
                    Map<String, Integer> seatTiers = new HashMap<>(concert.getSeatTiersMap());
                    seatTiers.put(request.getTier(), seatTiers.getOrDefault(request.getTier(), 0) + request.getCount());
                    concert.putAllSeatTiers(seatTiers);
                    // Set price if provided and > 0
                    if (request.getPrice() > 0) {
                        Map<String, Double> prices = new HashMap<>(concert.getPricesMap());
                        prices.put(request.getTier(), request.getPrice());
                        concert.putAllPrices(prices);
                    }
                }
                concerts.put(concertId, concert);
                ConcertService.ConcertResponse response = ConcertService.ConcertResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Ticket stock updated.")
                        .setConcert(concert.build())
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } finally {
//                concertLock.releaseLock();
            }
        } catch (Exception e) {
            ConcertService.ConcertResponse response = ConcertService.ConcertResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Distributed lock error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void updateTicketPrice(ConcertService.UpdateTicketPriceRequest request, StreamObserver<ConcertService.ConcertResponse> responseObserver) {
        System.out.println("[Command] Received updateTicketPrice request - Concert ID: " + request.getConcertId() + 
            ", Tier: " + request.getTier() + ", Price: " + request.getPrice());
        try {
//            concertLock.acquireLock();
            try {
                String concertId = request.getConcertId();
                ConcertService.Concert.Builder concert = concerts.get(concertId);
                if (concert == null) {
                    ConcertService.ConcertResponse response = ConcertService.ConcertResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage("Concert not found.")
                            .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    return;
                }
                Map<String, Double> prices = new HashMap<>(concert.getPricesMap());
                prices.put(request.getTier(), request.getPrice());
                concert.putAllPrices(prices);

                concertServer.setConcertData(concertId, request.getTier(), request.getPrice());

                concerts.put(concertId, concert);
                ConcertService.ConcertResponse response = ConcertService.ConcertResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Ticket price updated.")
                        .setConcert(concert.build())
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } finally {
//                concertLock.releaseLock();
            }
        } catch (Exception e) {
            ConcertService.ConcertResponse response = ConcertService.ConcertResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Distributed lock error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void reserveTickets(ConcertService.ReserveTicketsRequest request, StreamObserver<ConcertService.ReservationResponse> responseObserver) {
        System.out.println("[Command] Received reserveTickets request - Concert ID: " + request.getConcertId() + 
            ", Tier: " + request.getTier() + ", Count: " + request.getCount() + 
            ", After Party: " + request.getAfterParty() + ", Customer ID: " + request.getCustomerId());
        
        if (concertServer.isLeader()) {
            // Act as primary
            try {
                System.out.println("Reserving tickets as Primary");
                String operationId = "reserve_tickets_" + UUID.randomUUID().toString();
                startDistributedTx(operationId, request);
                updateSecondaryServers(operationId, request);
                ((DistributedTxCoordinator)concertServer.getTransaction()).perform();
            } catch (Exception e) {
                System.out.println("Error while reserving tickets: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            // Act as secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Reserving tickets on secondary, on Primary's command");
                String operationId = "reserve_tickets_" + UUID.randomUUID().toString();
                startDistributedTx(operationId, request);
                ((DistributedTxParticipant)concertServer.getTransaction()).voteCommit();
            } else {
                // Forward to primary
                ConcertService.ReservationResponse response = callPrimary(request);
                if (response.getSuccess()) {
                    transactionStatus = true;
                }
            }
        }

        ConcertService.ReservationResponse response = ConcertService.ReservationResponse.newBuilder()
                .setSuccess(transactionStatus)
                .setMessage(transactionStatus ? "Tickets reserved successfully." : "Failed to reserve tickets.")
                .setReservationId(UUID.randomUUID().toString())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void bulkReserve(ConcertService.BulkReserveRequest request, StreamObserver<ConcertService.ReservationResponse> responseObserver) {
        System.out.println("[Command] Received bulkReserve request - Concert ID: " + request.getConcertId() + 
            ", Tier: " + request.getTier() + ", Count: " + request.getCount() + 
            ", After Party: " + request.getAfterParty() + ", Group ID: " + request.getGroupId());
        try {
//            reservationLock.acquireLock();
            try {
                String concertId = request.getConcertId();
                ConcertService.Concert.Builder concert = concerts.get(concertId);
                if (concert == null) {
                    ConcertService.ReservationResponse response = ConcertService.ReservationResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage("Concert not found.")
                            .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    return;
                }
                
                synchronized (concert) {
                    int availableSeats = concert.getSeatTiersMap().getOrDefault(request.getTier(), 0);
                    int availableAfterParty = concert.getAfterPartyTickets();
                    
                    if (request.getCount() > availableSeats) {
                        ConcertService.ReservationResponse response = ConcertService.ReservationResponse.newBuilder()
                                .setSuccess(false)
                                .setMessage("Not enough seats available.")
                                .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        return;
                    }
                    
                    if (request.getAfterParty() && request.getCount() > availableAfterParty) {
                        ConcertService.ReservationResponse response = ConcertService.ReservationResponse.newBuilder()
                                .setSuccess(false)
                                .setMessage("Not enough after-party tickets available.")
                                .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        return;
                    }
                    
                    // Update seat tiers
                    Map<String, Integer> seatTiers = new HashMap<>(concert.getSeatTiersMap());
                    seatTiers.put(request.getTier(), availableSeats - request.getCount());
                    concert.putAllSeatTiers(seatTiers);
                    
                    // Update after-party tickets if needed
                    if (request.getAfterParty()) {
                        concert.setAfterPartyTickets(availableAfterParty - request.getCount());
                    }
                    
                    // Save the updated concert
                    concerts.put(concertId, concert);
                    
                    // Create and store the reservation
                    String reservationId = UUID.randomUUID().toString();
                    ConcertService.ReservationResponse response = ConcertService.ReservationResponse.newBuilder()
                            .setSuccess(true)
                            .setMessage("Bulk reservation successful.")
                            .setReservationId(reservationId)
                            .build();
                    reservations.put(reservationId, response);
                    
                    // Save the data to disk
                    saveData();
                    
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }
            } finally {
//                reservationLock.releaseLock();
            }
        } catch (Exception e) {
            ConcertService.ReservationResponse response = ConcertService.ReservationResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Distributed lock error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    // Expose concerts map for query service
    public Map<String, ConcertService.Concert.Builder> getConcerts() {
        return concerts;
    }

}