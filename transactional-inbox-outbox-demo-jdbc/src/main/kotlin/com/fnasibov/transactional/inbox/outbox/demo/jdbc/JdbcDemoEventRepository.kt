package com.fnasibov.transactional.inbox.outbox.demo.jdbc

import org.springframework.data.repository.ListCrudRepository
import org.springframework.stereotype.Repository
import java.util.UUID

@Repository
interface JdbcDemoEventRepository : ListCrudRepository<JdbcDemoEvent, UUID>
