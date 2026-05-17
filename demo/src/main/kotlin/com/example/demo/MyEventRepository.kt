package com.example.demo

import org.springframework.data.repository.reactive.ReactiveCrudRepository
import java.util.UUID

interface MyEventRepository : ReactiveCrudRepository<MyEvent, UUID>{
}