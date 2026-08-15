INSERT INTO jdbc_demo_events (
    id,
    status,
    created_at,
    retry_count,
    priority,
    payload
) VALUES (
    '00000000-0000-0000-0000-000000000002',
    'PENDING',
    CURRENT_TIMESTAMP,
    0,
    10,
    'Hello from the transactional inbox/outbox JDBC demo'
)
ON CONFLICT (id) DO NOTHING;
