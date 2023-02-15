package client;

import contractservice.Identifier;
import io.grpc.stub.StreamObserver;

public class ClientStream implements StreamObserver<Identifier> {

    private String identification;
    private boolean isComplete = false;

    @Override
    public void onNext(Identifier identifier) {
        identification = identifier.getIdentification();
    }

    @Override
    public void onError(Throwable throwable) {
        System.out.println("Error receiving the message!!");
        isComplete = true;
    }

    @Override
    public void onCompleted() {
        isComplete = true;
    }

    public boolean isComplete() {
        return this.isComplete;
    }

    public String getIdentification() {
        return this.identification;
    }
}
