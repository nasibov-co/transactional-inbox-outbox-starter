package com.example.demo

import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/my")
class MyController(private val repository: MyEventRepository) {

    @PostMapping
    suspend fun save(): MyEvent {
        return repository.save(MyEvent(aggregateId="aggregateId", payload = "payload ${kotlin.random.Random.nextInt()}")).awaitSingle()
    }
}