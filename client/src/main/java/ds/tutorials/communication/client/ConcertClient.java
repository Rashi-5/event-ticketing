package ds.tutorials.communication.client;

import concert.ConcertQueryServiceGrpc;
import concert.ConcertCommandServiceGrpc;
import concert.ConcertService.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import distributed.NameServiceClient;
import distributed.NameServiceClient.ServiceDetails;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ConcertClient {
    private static final String SERVICE_NAME = "concert-service";
    private static final String PROTOCOL = "grpc";
    private static final int RETRY_DELAY_MS = 5000;
    
    private final NameServiceClient nameServiceClient;
    private ManagedChannel channel;
    private ConcertQueryServiceGrpc.ConcertQueryServiceBlockingStub queryStub;
    private ConcertCommandServiceGrpc.ConcertCommandServiceBlockingStub commandStub;
    
    public ConcertClient(String nameServiceAddress) throws IOException {
        this.nameServiceClient = new NameServiceClient(nameServiceAddress);
        connectToServer();
    }
    
    private void connectToServer() throws IOException {
        try {
            ServiceDetails serviceDetails = nameServiceClient.findService(SERVICE_NAME);
            channel = ManagedChannelBuilder.forAddress(serviceDetails.getIPAddress(), serviceDetails.getPort())
                    .usePlaintext()
                    .build();
            queryStub = ConcertQueryServiceGrpc.newBlockingStub(channel);
            commandStub = ConcertCommandServiceGrpc.newBlockingStub(channel);
        } catch (InterruptedException e) {
            throw new IOException("Failed to connect to server", e);
        }
    }
    
    private void reconnectIfNeeded() {
        if (channel.isShutdown() || channel.isTerminated()) {
            try {
                connectToServer();
            } catch (IOException e) {
                System.err.println("Failed to reconnect to server: " + e.getMessage());
            }
        }
    }
    
    public void shutdown() throws InterruptedException {
        if (channel != null) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
    
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: ConcertClient <nameServiceAddress>");
            System.exit(1);
        }
        
        try {
            ConcertClient client = new ConcertClient(args[0]);
            Scanner scanner = new Scanner(System.in);

            System.out.println("Welcome to the Concert Ticket Reservation System");
            System.out.println("Select your role:");
            System.out.println("1. Organizer");
            System.out.println("2. Clerk");
            System.out.println("3. Coordinator");
            System.out.println("4. Customer");
            System.out.print("Enter role number (1-4): ");
            String roleInput = scanner.nextLine().trim();

            String role = "";
            switch (roleInput) {
                case "1":
                    role = "organizer";
                    break;
                case "2":
                    role = "clerk";
                    break;
                case "3":
                    role = "coordinator";
                    break;
                case "4":
                    role = "customer";
                    break;
                default:
                    System.out.println("Invalid role selection. Exiting.");
                    System.exit(1);
            }

            System.out.println("\nAvailable Options:");
            switch (role) {
                case "organizer":
                    System.out.println("1. List Concerts Details");
                    System.out.println("2. Add Concert");
                    System.out.println("3. Update Concert");
                    System.out.println("4. Cancel Concert");
                    System.out.println("6. Add/Update Seat Tier");
                    break;
                case "clerk":
                    System.out.println("1. List Concerts Details");
                    System.out.println("6. Add/Update Seat Tier");
                    System.out.println("7. Add After-Party Tickets");
                    System.out.println("9. Apply Discount");
                    break;
                case "coordinator":
                    System.out.println("1. List Concerts Details");
                    System.out.println("8. Bulk Ticket Reservation");
                    break;
                case "customer":
                    System.out.println("1. List Concerts Details");
                    System.out.println("5. Reserve Tickets");
                    break;
            }

            while (true) {
                client.reconnectIfNeeded();

                System.out.println("\nConcert Ticket Reservation System");
                System.out.println("1. List Concerts Details");
                System.out.println("2. Add Concert");
                System.out.println("3. Update Concert");
                System.out.println("4. Cancel Concert");
                System.out.println("5. Reserve Tickets");
                System.out.println("6. Add/Update Seat Tier");
                System.out.println("7. Add After-Party Tickets");
                System.out.println("8. Bulk Reserve (Coordinator)");
                System.out.println("9. Add Discount for the Tickets");
                System.out.println("0. Exit");
                System.out.print("Choose an option: ");
                
                String option = scanner.nextLine();

                try {
                    switch (option) {
                        case "1":
                            ListConcertsResponse response = client.queryStub.listConcerts(Empty.newBuilder().build());
                            System.out.println("Available Concerts:");
                            for (Concert concert : response.getConcertsList()) {
                                System.out.println("- " + concert.getName() + " (ID: " + concert.getId() + ")");
                                System.out.println("  Date: " + concert.getDate());
                                System.out.println("  Seat Tiers: " + concert.getSeatTiersMap());
                                System.out.println("  Max Seat Count: " + concert.getMaxTicketCount());
                                System.out.println("  After-Party Tickets: " + concert.getAfterPartyTickets());
                                System.out.println("  Prices: " + concert.getPricesMap());
                            }
                            break;
                        case "2":
                            if (role.equals("organizer")){
                                System.out.print("Concert Name: ");
                                String name = scanner.nextLine();
                                System.out.print("How many Tickets (max): ");
                                int tickets = Integer.parseInt(scanner.nextLine());
                                System.out.print("How many After party Tickets: ");
                                int afterPartyTickets = Integer.parseInt(scanner.nextLine());
                                if (afterPartyTickets > tickets) {
                                    System.out.println("Error: After-party tickets cannot exceed total tickets.");
                                    break;
                                }
                                System.out.print("Date (YYYY-MM-DD): ");
                                String date = scanner.nextLine();
                                LocalDate concertDate;
                                try {
                                    concertDate = LocalDate.parse(date, DateTimeFormatter.ISO_LOCAL_DATE);
                                    if (concertDate.isBefore(LocalDate.now())) {
                                        System.out.println("Error: The concert date cannot be in the past.");
                                        break;
                                    }
                                } catch (DateTimeParseException e) {
                                    System.out.println("Error: Invalid date format. Please use YYYY-MM-DD.");
                                    break;
                                }
                                String id = UUID.randomUUID().toString();
                                Concert concert = Concert.newBuilder()
                                    .setId(id)
                                    .setName(name)
                                    .setDate(date)
                                    .setMaxTicketCount(tickets)
                                    .setAfterPartyTickets(afterPartyTickets)
                                    .build();
                                AddConcertRequest req = AddConcertRequest.newBuilder().setConcert(concert).build();
                                ConcertResponse resp = client.commandStub.addConcert(req);
                                System.out.println(resp.getMessage());
                            } else {
                                System.out.println("Invalid option for your role.");
                            }
                            break;
                        case "3":
                            if (role.equals("organizer")) {
                                System.out.print("Concert ID to update: ");
                                String idToUpdate = scanner.nextLine();
                                System.out.print("New Name: ");
                                String newName = scanner.nextLine();
                                System.out.print("How many Tickets (max): ");
                                int tickets = Integer.parseInt(scanner.nextLine());
                                System.out.print("How many After party Tickets: ");
                                int afterPartyTickets = Integer.parseInt(scanner.nextLine());
                                if (afterPartyTickets > tickets) {
                                    System.out.println("Error: After-party tickets cannot exceed total tickets.");
                                    break;
                                }
                                System.out.print("New Date (YYYY-MM-DD): ");
                                String newDate = scanner.nextLine();
                                LocalDate concertDate;
                                try {
                                    concertDate = LocalDate.parse(newDate, DateTimeFormatter.ISO_LOCAL_DATE);
                                    if (concertDate.isBefore(LocalDate.now())) {
                                        System.out.println("Error: The concert date cannot be in the past.");
                                        break;
                                    }
                                } catch (DateTimeParseException e) {
                                    System.out.println("Error: Invalid date format. Please use YYYY-MM-DD.");
                                    break;
                                }
                                Concert updatedConcert = Concert.newBuilder()
                                        .setId(idToUpdate)
                                        .setName(newName)
                                        .setMaxTicketCount(tickets)
                                        .setAfterPartyTickets(afterPartyTickets)
                                        .setDate(newDate)
                                        .build();
                                UpdateConcertRequest updateReq = UpdateConcertRequest.newBuilder().setConcert(updatedConcert).build();
                                ConcertResponse updateResp = client.commandStub.updateConcert(updateReq);
                                System.out.println(updateResp.getMessage());
                            } else {
                                System.out.println("Invalid option for your role.");
                            }
                            break;
                        case "4":
                            if (role.equals("organizer")) {
                                System.out.print("Concert ID to cancel: ");
                                String cancelId = scanner.nextLine();
                                CancelConcertRequest cancelReq = CancelConcertRequest.newBuilder().setConcertId(cancelId).build();
                                ConcertResponse cancelResp = client.commandStub.cancelConcert(cancelReq);
                                System.out.println(cancelResp.getMessage());
                            } else {
                                System.out.println("Invalid option for your role.");
                            }
                            break;
                        case "5":
                            if (role.equals("customer")) {
                                System.out.print("Concert ID: ");
                                String concertId = scanner.nextLine();
                                System.out.print("Seat Tier: ");
                                String tier = scanner.nextLine();
                                System.out.print("Number of Tickets: ");
                                int count = Integer.parseInt(scanner.nextLine());
                                System.out.print("After Party (true/false): ");
                                boolean afterParty = Boolean.parseBoolean(scanner.nextLine());
                                System.out.print("Customer ID: ");
                                String customerId = scanner.nextLine();
                                ReserveTicketsRequest reserveReq = ReserveTicketsRequest.newBuilder()
                                        .setConcertId(concertId)
                                        .setTier(tier)
                                        .setCount(count)
                                        .setAfterParty(afterParty)
                                        .setCustomerId(customerId)
                                        .build();
                                ReservationResponse reserveResp = client.commandStub.reserveTickets(reserveReq);
                                System.out.println(reserveResp.getMessage() + " Reservation ID: " + reserveResp.getReservationId());
                            } else {
                                System.out.println("Invalid option for your role.");
                            }
                            break;
                        case "6":
                            if (role.equals("clerk") || role.equals("organizer")) {
                                System.out.print("Concert ID: ");
                                String concertIdToUpdate = scanner.nextLine();
                                System.out.print("Seat Tier Name: ");
                                String tierToUpdate = scanner.nextLine();
                                System.out.print("Number of Seats to Add: ");
                                int seatsToAdd = Integer.parseInt(scanner.nextLine());
                                System.out.print("Price: ");
                                double price = Double.parseDouble(scanner.nextLine());
                                AddTicketStockRequest stockReq = AddTicketStockRequest.newBuilder()
                                        .setConcertId(concertIdToUpdate)
                                        .setTier(tierToUpdate)
                                        .setCount(seatsToAdd)
                                        .setAfterParty(false)
                                        .build();
                                ConcertResponse stockResp = client.commandStub.addTicketStock(stockReq);
                                UpdateTicketPriceRequest priceReq = UpdateTicketPriceRequest.newBuilder()
                                        .setConcertId(concertIdToUpdate)
                                        .setTier(tierToUpdate)
                                        .setPrice(price)
                                        .build();
                                ConcertResponse priceResp = client.commandStub.updateTicketPrice(priceReq);
                                System.out.println(stockResp.getMessage());
                                System.out.println(priceResp.getMessage());
                            } else {
                                System.out.println("Invalid option for your role.");
                            }
                            break;
                        case "7":
                            if (role.equals("clerk")) {
                                System.out.print("Concert ID: ");
                                String concertIdToAddAfterParty = scanner.nextLine();
                                System.out.print("Number of After-Party Tickets to Add: ");
                                int afterPartyTicketsToAdd = Integer.parseInt(scanner.nextLine());
                                AddTicketStockRequest afterPartyReq = AddTicketStockRequest.newBuilder()
                                        .setConcertId(concertIdToAddAfterParty)
                                        .setTier("")
                                        .setCount(afterPartyTicketsToAdd)
                                        .setAfterParty(true)
                                        .build();
                                ConcertResponse afterPartyResp = client.commandStub.addTicketStock(afterPartyReq);
                                System.out.println(afterPartyResp.getMessage());
                            } else {
                                System.out.println("Invalid option for your role.");
                            }
                            break;
                        case "8":
                            if (role.equals("coordinator")) {
                                System.out.print("Concert ID: ");
                                String concertIdToBulkReserve = scanner.nextLine();
                                System.out.print("Seat Tier: ");
                                String tierToBulkReserve = scanner.nextLine();
                                System.out.print("Number of Tickets: ");
                                int ticketsToReserve = Integer.parseInt(scanner.nextLine());
                                System.out.print("After Party (true/false): ");
                                boolean afterPartyForBulk = Boolean.parseBoolean(scanner.nextLine());
                                System.out.print("Group ID: ");
                                String groupId = scanner.nextLine();
                                BulkReserveRequest bulkReq = BulkReserveRequest.newBuilder()
                                        .setConcertId(concertIdToBulkReserve)
                                        .setTier(tierToBulkReserve)
                                        .setCount(ticketsToReserve)
                                        .setAfterParty(afterPartyForBulk)
                                        .setGroupId(groupId)
                                        .build();
                                ReservationResponse bulkResp = client.commandStub.bulkReserve(bulkReq);
                                System.out.println(bulkResp.getMessage() + " Reservation ID: " + bulkResp.getReservationId());
                            } else {
                                System.out.println("Invalid option for your role.");
                            }
                            break;
                        case "9":
                            if (role.equals("clerk")) {
                                System.out.print("Concert ID: ");
                                String concertIdToDiscount = scanner.nextLine();
                                System.out.print("Seat Tier (leave empty to apply to all tiers): ");
                                String tierToDiscount = scanner.nextLine();
                                System.out.print("Discount Percentage (e.g., 10 for 10%): ");
                                double discountPercentage = Double.parseDouble(scanner.nextLine());

                                ApplyDiscountRequest discountRequest = ApplyDiscountRequest.newBuilder()
                                        .setConcertId(concertIdToDiscount)
                                        .setTier(tierToDiscount)
                                        .setDiscountPercentage(discountPercentage)
                                        .build();

                                ConcertResponse discountResponse = client.commandStub.applyDiscount(discountRequest);
                                System.out.println(discountResponse.getMessage());
                            } else {
                                System.out.println("Invalid option for your role.");
                            }
                            break;
                        case "0":
                            client.shutdown();
                            return;
                        default:
                            System.out.println("Invalid option.");
                    }
                } catch (Exception e) {
                    System.err.println("Error: " + e.getMessage());
                    Thread.sleep(RETRY_DELAY_MS);
                }
            }
        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
