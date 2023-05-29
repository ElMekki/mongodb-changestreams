package com.paoue74.changestreams;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.Objects;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
import static org.springframework.data.mongodb.core.query.Criteria.where;

@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class JobChangeStreamService {

  private final ReactiveMongoTemplate reactiveMongoTemplate;

  public Flux<Notification> watchForJobCollectionChanges() {
    ChangeStreamOptions options = ChangeStreamOptions.builder()
        .filter(newAggregation(Job.class, match(where("operationType").in("insert", "update","replace"))
        ))
        .build();
//                Aggregation.project("id", "fullDocument", "ns", "documentKey")
    // return a flux that watches the changestream and returns a notification object
    return reactiveMongoTemplate.changeStream("jobs", options, Job.class)
        .filter(Objects::nonNull)
        .map(event -> event.getRaw())
        .map(doc -> new Notification()
            .withType(doc.getOperationType().getValue())
            .withJob(doc.getFullDocument())
        )
        .doOnNext(notification -> log.info("{}", notification))
        .doOnError(throwable -> log.error("Error with the jobs changestream event: " + throwable.getMessage(), throwable));
  }
}
