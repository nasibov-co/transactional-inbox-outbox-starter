package com.fnasibov.transactional.inbox.outbox.demo

import org.springframework.data.repository.kotlin.CoroutineCrudRepository
import org.springframework.stereotype.Repository
import java.util.UUID

@Repository
interface BatchDemoEventRepository : CoroutineCrudRepository<BatchDemoEvent, UUID>