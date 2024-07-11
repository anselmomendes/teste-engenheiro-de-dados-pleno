CREATE table if not exists order_number
(
    id SERIAL PRIMARY KEY,
    order_number VARCHAR(32)
);
CREATE table if not exists order_tracking_code
(
    id SERIAL PRIMARY KEY,
    order_tracking_code VARCHAR(32)
);

CREATE table if not exists order_status
(
    id SERIAL PRIMARY KEY,
    order_status VARCHAR(32)
);

CREATE table if not exists order_from
(
    id SERIAL PRIMARY KEY,
    order_from VARCHAR(32)
);

CREATE table if not exists order_to
(
    id SERIAL PRIMARY KEY,
    order_to VARCHAR(32)
);

create table if not exists order_main
(
    id SERIAL PRIMARY KEY,
    oid_id INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_sync_tracker TIMESTAMP,
    order_created_at TIMESTAMP,
    order_tracking_code_id INTEGER,
    order_status_id INTEGER,
    order_description VARCHAR(200),
    order_tracker_type VARCHAR(32),
    order_from_id INTEGER,
    order_to_id INTEGER,
    FOREIGN KEY (oid_id) REFERENCES order_number(id),
    FOREIGN KEY (order_tracking_code_id) REFERENCES order_tracking_code(id),
    FOREIGN KEY (order_status_id) REFERENCES order_status(id),
    FOREIGN KEY (order_from_id) REFERENCES order_from(id),
    FOREIGN KEY (order_to_id) REFERENCES order_to(id)
);