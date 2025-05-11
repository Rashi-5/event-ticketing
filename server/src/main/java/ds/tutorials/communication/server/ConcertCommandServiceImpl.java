package ds.tutorials.communication.server;

import concert.ConcertCommandServiceGrpc;
import concert.ConcertService;
import io.grpc.stub.StreamObserver;
import ds.tutorials.synchronization.DistributedTxCoordinator;
import ds.tutorials.synchronization.DistributedTxListener;
import ds.tutorials.synchronization.DistributedLock;

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
//    private final DistributedLock concertLock;
//    private final DistributedLock reservationLock;

    public ConcertCommandServiceImpl(String nameServiceAddress, String dataDir) {

        this.nodeId = UUID.randomUUID().toString();
        this.coordinator = new DistributedTxCoordinator(this);
        this.dataDir = dataDir;
        this.nameServiceAddress = nameServiceAddress;

//        try {
//            this.concertLock = new DistributedLock("concert-lock");
//            this.reservationLock = new DistributedLock("reservation-lock");
//        } catch (IOException | KeeperException | InterruptedException e) {
//            throw new RuntimeException("Failed to initialize distributed locks", e);
//        }
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
                Map<String, ConcertService.Concert.Builder> loadedConcerts = (Map<String, ConcertService.Concert.Builder>) ois.readObject();
                concerts.putAll(loadedConcerts);
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
            oos.writeObject(new HashMap<>(concerts));
        }
    }

    private void saveReservations() throws IOException {
        File reservationsFile = new File(dataDir, "reservations.dat");
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(reservationsFile))) {
            oos.writeObject(new HashMap<>(reservations));
        }
    }

    @Override
    public void onGlobalCommit() {
        saveData();
    }

    @Override
    public void onGlobalAbort() {
        // No need to save data on abort
    }

    private boolean performAtomicOperation(String operationId, Runnable operation) {
        try {
            coordinator.start(operationId, nodeId);
            operation.run();
            return coordinator.perform();
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void addConcert(ConcertService.AddConcertRequest request, StreamObserver<ConcertService.ConcertResponse> responseObserver) {
        System.out.println("[Command] Received addConcert request - Concert: " + request.getConcert().getName() + 
            " (ID: " + request.getConcert().getId() + ", Date: " + request.getConcert().getDate() + ")");
        try {
//            concertLock.acquireLock();
            try {
                ConcertService.Concert concert = request.getConcert();
                concerts.put(concert.getId(), concert.toBuilder());
                ConcertService.ConcertResponse response = ConcertService.ConcertResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Concert added successfully.")
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
        try {
//            reservationLock.acquireLock();
            try {
                final ConcertService.ReservationResponse[] reservationHolder = new ConcertService.ReservationResponse[1];
                String operationId = "reserve_" + UUID.randomUUID().toString();
                boolean success = performAtomicOperation(operationId, () -> {
                    String concertId = request.getConcertId();
                    ConcertService.Concert.Builder concert = concerts.get(concertId);
                    if (concert == null) {
                        throw new RuntimeException("Concert not found");
                    }
                    
                    synchronized (concert) {
                        int availableSeats = concert.getSeatTiersMap().getOrDefault(request.getTier(), 0);
                        int availableAfterParty = concert.getAfterPartyTickets();
                        
                        if (request.getCount() > availableSeats) {
                            throw new RuntimeException("Not enough seats available");
                        }
                        
                        if (request.getAfterParty() && request.getCount() > availableAfterParty) {
                            throw new RuntimeException("Not enough after-party tickets available");
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
                                .setMessage("Reservation successful")
                                .setReservationId(reservationId)
                                .build();
                        reservations.put(reservationId, response);
                        reservationHolder[0] = response;
                        
                        // Save the data to disk
                        saveData();
                    }
                });
                
                if (success && reservationHolder[0] != null) {
                    responseObserver.onNext(reservationHolder[0]);
                } else {
                    responseObserver.onNext(ConcertService.ReservationResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage("Failed to reserve tickets")
                            .build());
                }
                responseObserver.onCompleted();
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