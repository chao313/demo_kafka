package demo.kafka.controller.response;

import lombok.Data;
import org.apache.kafka.clients.admin.ConsumerGroupListing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Data
public class ConsumerGroupListingResponse {
    private String groupId;
    private boolean isSimpleConsumerGroup;

    public ConsumerGroupListingResponse(String groupId, boolean isSimpleConsumerGroup) {
        this.groupId = groupId;
        this.isSimpleConsumerGroup = isSimpleConsumerGroup;
    }

    /**
     * 批量转换
     */
    public static Collection<ConsumerGroupListingResponse> addAll(Collection<ConsumerGroupListing> consumerGroupListings) {
        List<ConsumerGroupListingResponse> consumerGroupListingResponses = new ArrayList<>();
        consumerGroupListings.forEach(consumerGroupListing -> {
            consumerGroupListingResponses.add(new ConsumerGroupListingResponse(consumerGroupListing.groupId(), consumerGroupListing.isSimpleConsumerGroup()));
        });
        return consumerGroupListingResponses;
    }
}
