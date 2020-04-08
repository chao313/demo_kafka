package demo.kafka.controller.response;

import lombok.Data;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

@Data
public class ListConsumerGroupsResultResponse {

    private Collection<ConsumerGroupListingResponse> all;
    private Collection<ConsumerGroupListingResponse> valid;
    private Collection<Throwable> errors;

    public ListConsumerGroupsResultResponse(ListConsumerGroupsResult listConsumerGroupsResult) throws ExecutionException, InterruptedException {
        this.all = ConsumerGroupListingResponse.addAll(listConsumerGroupsResult.all().get());
        this.valid = ConsumerGroupListingResponse.addAll(listConsumerGroupsResult.valid().get());
        this.errors = listConsumerGroupsResult.errors().get();
    }

}
