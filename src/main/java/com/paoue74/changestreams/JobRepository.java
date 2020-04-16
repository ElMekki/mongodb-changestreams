package com.paoue74.changestreams;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

public interface JobRepository extends ReactiveMongoRepository<Job, String> {
}
