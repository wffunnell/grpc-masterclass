package grpc.greeting.client;

import com.proto.greet.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class GreetingClient {

    private Greeting stubGreeting = Greeting.newBuilder()
            .setFirstName("Will")
            .setLastName("Funnell")
            .build();

    public static void main(String[] args) {
        System.out.println("Hello, I am a gRPC client");
        GreetingClient gc = new GreetingClient();
        gc.run();
    }

    public void run() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext() //added due to lack of SSL setup
                .build();

        doUnaryCall(channel);
        doServerStreaming(channel);
        doClientStreaming(channel);
        doBiDirecetionalStreaming(channel);

        System.out.println("Shutting down channel");
        channel.shutdown();
    }

    private void doUnaryCall(ManagedChannel channel) {
        System.out.println("Unary call");

        GreetRequest request = GreetRequest.newBuilder()
                .setGreeting(stubGreeting)
                .build();

        GreetServiceGrpc.GreetServiceBlockingStub greetService = GreetServiceGrpc.newBlockingStub(channel);

        GreetResponse greetResponse = greetService.greet(request);
        System.out.println("Unary response: " + greetResponse.getResult());
    }

    private void doServerStreaming(ManagedChannel channel) {
        System.out.println("Server streaming");

        GreetManyTimesRequest greetManyTimesRequest = GreetManyTimesRequest.newBuilder()
                .setGreeting(stubGreeting)
                .build();

        GreetServiceGrpc.GreetServiceBlockingStub greetClient = GreetServiceGrpc.newBlockingStub(channel);

        greetClient.greetManyTimes(greetManyTimesRequest)
                .forEachRemaining(greetManyTimesResponse -> {
                    System.out.println(greetManyTimesResponse.getResult());
                });
    }

    private void doClientStreaming(ManagedChannel channel) {
        System.out.println("Client streaming");

        CountDownLatch latch = new CountDownLatch(1);

        GreetServiceGrpc.GreetServiceStub asyncClient = GreetServiceGrpc.newStub(channel);

        StreamObserver<LongGreetResponse> responseObserver = new StreamObserver<LongGreetResponse>() {
            @Override
            public void onNext(LongGreetResponse response) {
                System.out.println("Received Long Greet response: " + response.getResult());
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onCompleted() {
                System.out.println("Server has completed sending something");
                latch.countDown();
            }
        };

        StreamObserver<LongGreetRequest> requestObserver = asyncClient.longGreet(responseObserver);
        for (int i = 0; i < 10; i++) {
            Greeting greeting = Greeting.newBuilder()
                    .setFirstName("Will " + i)
                    .setLastName("Funnell")
                    .build();

            LongGreetRequest longGreetRequest = LongGreetRequest.newBuilder()
                    .setGreeting(greeting)
                    .build();

            requestObserver.onNext(longGreetRequest);
        }

        requestObserver.onCompleted();

        try {
            latch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void doBiDirecetionalStreaming(ManagedChannel channel) {
        System.out.println("Bi-directional streaming");

        CountDownLatch latch = new CountDownLatch(3);

        GreetServiceGrpc.GreetServiceStub asyncClient = GreetServiceGrpc.newStub(channel);

        StreamObserver<GreetEveryoneResponse> responseObserver = new StreamObserver<GreetEveryoneResponse>() {
            @Override
            public void onNext(GreetEveryoneResponse value) {
                System.out.println("Received Greet Everyone response: " + value.getResult());
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onCompleted() {
                System.out.println("Server has completed greeting everyone");
                latch.countDown();
            }
        };

        StreamObserver<GreetEveryoneRequest> requestObserver = asyncClient.greetEveryone(responseObserver);

        GreetEveryoneRequest steve = GreetEveryoneRequest.newBuilder()
                .setGreeting(Greeting.newBuilder().setFirstName("Steve"))
                .build();
        requestObserver.onNext(steve);

        GreetEveryoneRequest bill = GreetEveryoneRequest.newBuilder()
                .setGreeting(Greeting.newBuilder().setFirstName("Bill"))
                .build();
        requestObserver.onNext(bill);

        GreetEveryoneRequest john = GreetEveryoneRequest.newBuilder()
                .setGreeting(Greeting.newBuilder().setFirstName("John"))
                .build();
        requestObserver.onNext(john);

        requestObserver.onCompleted();

        try {
            latch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
