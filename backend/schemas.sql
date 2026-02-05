CREATE TABLE salespeople (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    branch VARCHAR(100),
    role VARCHAR(50)
);

CREATE TABLE sales_records (
    id INT AUTO_INCREMENT PRIMARY KEY,
    employee_id VARCHAR(50),
    vehicle_model VARCHAR(100),
    quantity INT,
    sale_date DATE,
    vehicle_type VARCHAR(50),

    CONSTRAINT fk_sales_records_employee
        FOREIGN KEY (employee_id)
        REFERENCES salespeople(id)
        ON DELETE CASCADE
);

CREATE TABLE incentive_rules (
    id VARCHAR(50) PRIMARY KEY,
    role VARCHAR(50),
    vehicle_type VARCHAR(50),
    min_units INT,
    max_units INT,
    incentive_amount FLOAT DEFAULT 0.0,
    bonus_per_unit FLOAT DEFAULT 0.0,
    valid_from DATE,
    valid_to DATE,
    rule_type VARCHAR(20) DEFAULT 'Structured',
    description TEXT
);

CREATE TABLE calculation_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    employee_id VARCHAR(50),
    period_month VARCHAR(20),
    total_incentive FLOAT,
    breakdown_json TEXT,
    status VARCHAR(20) DEFAULT 'Success',
    calculated_at DATE,

    CONSTRAINT fk_calculation_employee
        FOREIGN KEY (employee_id)
        REFERENCES salespeople(id)
        ON DELETE CASCADE
);

