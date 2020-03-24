package grpc.greeting.client;

import com.proto.dummy.DummyServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Client {

    public static void main(String[] args) {
        System.out.println("Hello, I am a gRPC client");

        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .build();

        System.out.println("Creating stub");
        DummyServiceGrpc.DummyServiceBlockingStub syncClient = DummyServiceGrpc.newBlockingStub(channel);

        //
        // DummyServiceGrpc.DummyServiceFutureStub futureStub = DummyServiceGrpc.newFutureStub(channel);

        //do something

        System.out.println("Shutting down channel");
        channel.shutdown();
    }
}
