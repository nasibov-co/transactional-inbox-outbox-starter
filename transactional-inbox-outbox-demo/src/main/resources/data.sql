INSERT INTO demo_events (
    id,
    status,
    created_at,
    retry_count,
    priority,
    payload
) VALUES (
    '00000000-0000-0000-0000-000000000001',
    'PENDING',
    CURRENT_TIMESTAMP,
    0,
    10,
    'Hello from the transactional inbox/outbox demo'
)
ON CONFLICT (id) DO NOTHING;

-- Three events sharing the same @BatchKey value: they are fetched and handled
-- together in a single BatchEventHandler.handleBatch invocation.
INSERT INTO batch_demo_events (
    id,
    status,
    created_at,
    retry_count,
    batch_key,
    payload
) VALUES
    (
        '00000000-0000-0000-0000-000000000011',
        'PENDING',
        CURRENT_TIMESTAMP,
        0,
        'account-42',
        'Batch demo event 1 for account-42'
    ),
    (
        '00000000-0000-0000-0000-000000000012',
        'PENDING',
        CURRENT_TIMESTAMP,
        0,
        'account-42',
        'Batch demo event 2 for account-42'
    ),
    (
        '00000000-0000-0000-0000-000000000013',
        'PENDING',
        CURRENT_TIMESTAMP,
        0,
        'account-42',
        'Batch demo event 3 for account-42'
    )
ON CONFLICT (id) DO NOTHING;
