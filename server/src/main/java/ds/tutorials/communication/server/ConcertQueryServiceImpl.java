package ds.tutorials.communication.server;

import concert.ConcertQueryServiceGrpc;
import concert.ConcertService;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.stream.Collectors;

public class ConcertQueryServiceImpl extends ConcertQueryServiceGrpc.ConcertQueryServiceImplBase {
    private final Map<String, ConcertService.Concert.Builder> concerts;
    private ConcertServer concertServer;

    public ConcertQueryServiceImpl(Map<String, ConcertService.Concert.Builder> concerts) {
        this.concerts = concerts;
    }

    @Override
    public void listConcerts(ConcertService.Empty request, StreamObserver<ConcertService.ListConcertsResponse> responseObserver) {
        ConcertService.ListConcertsResponse response = ConcertService.ListConcertsResponse.newBuilder()
                .addAllConcerts(concerts.values().stream().map(ConcertService.Concert.Builder::build).collect(Collectors.toList()))
                .build();
        System.out.println("Checking concert " );
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void checkConcert(ConcertService.getConcertRequest request, StreamObserver<ConcertService.ConcertsResponse> responseObserver) {
        ConcertService.ConcertsResponse response = ConcertService.ConcertsResponse.newBuilder()
                .build();

        System.out.println("Checking concert " + concertServer.getConcertData(request.getConcertId()));
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}