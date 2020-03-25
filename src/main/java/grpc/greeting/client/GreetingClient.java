package grpc.greeting.client;

import com.proto.dummy.DummyServiceGrpc;
import com.proto.greet.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Iterator;

public class GreetingClient {

    public static void main(String[] args) {
        System.out.println("Hello, I am a gRPC client");

        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext() //added due to lack of SSL setup
                .build();

        System.out.println("Creating stub");

        // DummyServiceGrpc.DummyServiceBlockingStub syncClient = DummyServiceGrpc.newBlockingStub(channel);
        // DummyServiceGrpc.DummyServiceFutureStub futureStub = DummyServiceGrpc.newFutureStub(channel);

        GreetServiceGrpc.GreetServiceBlockingStub greetClient = GreetServiceGrpc.newBlockingStub(channel);

        Greeting greeting = Greeting.newBuilder()
                .setFirstName("Will")
                .setLastName("Funnell")
                .build();
//
//        GreetRequest request = GreetRequest.newBuilder()
//                .setGreeting(greeting)
//                .build();
//
//        GreetResponse greetResponse = greetClient.greet(request);

        GreetManyTimesRequest greetManyTimesRequest = GreetManyTimesRequest.newBuilder()
                .setGreeting(greeting)
                .build();

        greetClient.greetManyTimes(greetManyTimesRequest)
                .forEachRemaining(greetManyTimesResponse -> {
                    System.out.println(greetManyTimesResponse.getResult());
                });

        System.out.println("Shutting down channel");
        channel.shutdown();
    }
}
