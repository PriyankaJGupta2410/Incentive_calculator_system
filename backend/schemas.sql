CREATE TABLE sales_transactions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    employee_id VARCHAR(50),
    branch VARCHAR(100),
    role VARCHAR(50),
    vehicle_model VARCHAR(100),
    vehicle_type VARCHAR(50),
    quantity INT,
    sale_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sales_upload_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_name VARCHAR(255),
    uploaded_by VARCHAR(100),
    total_rows INT,
    valid_rows INT,
    invalid_rows INT,
    upload_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sales_upload_errors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    error_message TEXT,
    csv_row_number  INT not null,
    raw_data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE incentive_rule_uploads (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_name VARCHAR(255),
    uploaded_by VARCHAR(100),
    upload_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_rows INT,
    valid_rows INT,
    invalid_rows INT,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE incentive_rules (
    id INT AUTO_INCREMENT PRIMARY KEY,

    rule_id VARCHAR(50),
    role VARCHAR(50),
    vehicle_type VARCHAR(100),

    min_qty INT,
    max_qty INT,

    base_amount DECIMAL(10,2),
    per_unit_amount DECIMAL(10,2),

    valid_from DATE,
    valid_to DATE,

    priority INT,

    upload_id INT,
    is_active BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_rule_lookup (role, vehicle_type),
    INDEX idx_validity (valid_from, valid_to),
    INDEX idx_priority (priority),

    FOREIGN KEY (upload_id)
        REFERENCES incentive_rule_uploads(id)
        ON DELETE CASCADE
);


CREATE TABLE incentive_rule_versions (
    id INT AUTO_INCREMENT PRIMARY KEY,

    rule_id VARCHAR(50),
    rule_snapshot JSON,
    upload_id INT,

    versioned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (upload_id)
        REFERENCES incentive_rule_uploads(id)
        ON DELETE CASCADE
);
