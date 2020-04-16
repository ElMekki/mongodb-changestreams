package com.paoue74.changestreams;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/jobs")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class JobController {

  private final JobRepository jobRepository;
  private final JobChangeStreamService jobChangeStreamService;

  @GetMapping
  public Flux<Job> get() {
    return jobRepository.findAll();
  }

  @PostMapping
  public Mono<Job> create(@RequestBody Job job){
    return jobRepository.insert(job); // To make sure an insert event will be created.
  }

  @PutMapping
  public Mono<Job> update(@RequestBody Job job) {
    return jobRepository.save(job);
  }

  @DeleteMapping("/{id}")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  public Mono<Void> delete(@PathVariable String id) {
    return jobRepository.deleteById(id);
  }

  @GetMapping("/watch")
  public Flux<ServerSentEvent<Notification>> watchJobAdded() {
    return jobChangeStreamService.watchForJobCollectionChanges()
        .map(job -> ServerSentEvent.<Notification>builder()
            .data(job)
            .build()
        );
  }
}
