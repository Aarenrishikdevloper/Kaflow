#include "../src/metadata/topic_manager.hpp"
#include "metadata/topic.hpp"

void seed_test_topics() {

    // -------------------------
    // Topic: orders
    // -------------------------

    TopicMetadata orders;
    orders.name = "orders";
    orders.topic_id[0] = std::byte{1};

    for (std::int32_t i = 0; i < 3; ++i) {

        partionMetadata partition;

        partition.partion_index = i;
        partition.leader_id = 1;
        partition.leader_epoch = 0;

        partition.replica_node = {1};
        partition.isr_nodes = {1};

        orders.partitions.push_back(partition);
    }

    topic_manager().add_topic(std::move(orders));

    // -------------------------
    // Topic: payments
    // -------------------------

    TopicMetadata payments;
    payments.name = "payments";
    payments.topic_id[0] = std::byte{2};

    for (std::int32_t i = 0; i < 2; ++i) {

        partionMetadata partition;

        partition.partion_index= i;
        partition.leader_id = 1;
        partition.leader_epoch = 0;
        
        partition.replica_node = {1};
        partition.isr_nodes = {1};

        payments.partitions.push_back(partition);
    }

    topic_manager().add_topic(std::move(payments));

    // -------------------------
    // Topic: users
    // -------------------------

    TopicMetadata users;
    users.name = "users";
    users.topic_id[0] = std::byte{3};

    for (std::int32_t i = 0; i < 2; ++i) {

        partionMetadata partition;

        partition.partion_index = i;
        partition.leader_id = 1;
        partition.leader_epoch = 0;

        partition.replica_node = {1};
        partition.isr_nodes = {1};

        users.partitions.push_back(partition);
    }

    topic_manager().add_topic(std::move(users));
}