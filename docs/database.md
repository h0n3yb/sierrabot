# PostgreSQL Database Documentation

## Overview
The autonomous transformation agent uses a local PostgreSQL database (version 14) with a read-only access pattern to ensure data safety. The database is structured to support common business scenarios while providing rich data for testing various transformation scenarios.

## Connection Details
- **Database Name**: `sierra_db`
- **Schema**: `app_data`
- **Read-only User**: `sierra_readonly`
- **Connection String Format**: 
```
postgresql://sierra_readonly:readonly_pass@localhost:5432/sierra_db
```

## Security Model
- Dedicated read-only user (`sierra_readonly`) with restricted permissions
- Schema-level access control via `GRANT` statements
- All write operations are prohibited for the application user
- Tables are owned by the postgres superuser but readable by `sierra_readonly`

## Schema: app_data

### Tables

#### 1. customers
Primary table storing customer information.

```sql
CREATE TABLE app_data.customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Sample data includes 7 customers with diverse profiles:
- High-value customers (Priya, Alex, John)
- Mid-range customers (Michael, Sarah)
- New customers (Jane, Emma)

#### 2. orders
Stores order information with customer relationship.

```sql
CREATE TABLE app_data.orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES app_data.customers(id),
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Order statuses include: 'completed', 'pending', 'processing', 'cancelled'

#### 3. products
Catalog of available products.

```sql
CREATE TABLE app_data.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(50) NOT NULL,
    in_stock BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Product categories:
- Electronics (Laptop Pro X, Wireless Earbuds, Smart Watch Elite)
- Appliances (Coffee Maker Deluxe)
- Fitness (Yoga Mat Premium, Protein Powder)

#### 4. order_items
Links orders to products, storing quantity and price information.

```sql
CREATE TABLE app_data.order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES app_data.orders(id),
    product_id INTEGER REFERENCES app_data.products(id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 5. customer_interactions
Tracks customer support and interaction history.

```sql
CREATE TABLE app_data.customer_interactions (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES app_data.customers(id),
    interaction_type VARCHAR(50) NOT NULL,
    description TEXT,
    status VARCHAR(20) NOT NULL,
    priority VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP
);
```

Interaction types include:
- Technical Support
- Product Review
- Shipping Issue
- Return Request
- Billing Dispute
- Order Modification

## Sample Queries

### 1. Customer Spending Analysis
```sql
SELECT 
    c.name,
    COUNT(DISTINCT o.id) as order_count,
    COUNT(DISTINCT ci.id) as interaction_count,
    SUM(oi.quantity * oi.unit_price) as total_spent,
    STRING_AGG(DISTINCT p.category, ', ') as categories_purchased
FROM app_data.customers c
LEFT JOIN app_data.orders o ON c.id = o.customer_id
LEFT JOIN app_data.order_items oi ON o.id = oi.order_id
LEFT JOIN app_data.products p ON oi.product_id = p.id
LEFT JOIN app_data.customer_interactions ci ON c.id = ci.customer_id
GROUP BY c.id, c.name
ORDER BY total_spent DESC;
```

### 2. Product Category Performance
```sql
SELECT 
    p.category,
    COUNT(DISTINCT o.id) as order_count,
    SUM(oi.quantity) as units_sold,
    SUM(oi.quantity * oi.unit_price) as revenue,
    AVG(oi.unit_price) as avg_unit_price
FROM app_data.products p
LEFT JOIN app_data.order_items oi ON p.id = oi.product_id
LEFT JOIN app_data.orders o ON oi.order_id = o.id
WHERE o.status = 'completed'
GROUP BY p.category
ORDER BY revenue DESC;
```

### 3. Customer Support Analysis
```sql
SELECT 
    c.name,
    ci.interaction_type,
    ci.priority,
    COUNT(*) as interaction_count,
    AVG(EXTRACT(EPOCH FROM (ci.resolved_at - ci.created_at))/3600) as avg_resolution_hours,
    COUNT(CASE WHEN ci.status = 'pending' THEN 1 END) as pending_issues
FROM app_data.customers c
JOIN app_data.customer_interactions ci ON c.id = ci.customer_id
GROUP BY c.name, ci.interaction_type, ci.priority
HAVING COUNT(*) > 0
ORDER BY avg_resolution_hours DESC;
```

### 4. Cross-Category Purchase Analysis
```sql
WITH customer_categories AS (
    SELECT DISTINCT
        c.id,
        c.name,
        p.category
    FROM app_data.customers c
    JOIN app_data.orders o ON c.id = o.customer_id
    JOIN app_data.order_items oi ON o.id = oi.order_id
    JOIN app_data.products p ON oi.product_id = p.id
)
SELECT 
    name,
    COUNT(DISTINCT category) as unique_categories,
    STRING_AGG(category, ', ') as purchased_categories
FROM customer_categories
GROUP BY id, name
HAVING COUNT(DISTINCT category) > 1
ORDER BY unique_categories DESC;
```

### 5. Order Status Distribution
```sql
SELECT 
    status,
    COUNT(*) as order_count,
    AVG(amount) as avg_order_value,
    SUM(amount) as total_value
FROM app_data.orders
GROUP BY status
ORDER BY order_count DESC;
```

## Common Transformation Scenarios

1. **Customer Analysis**
   - Customer lifetime value calculation
   - Purchase frequency patterns
   - Category preferences
   - Support ticket history
   - Cross-category buying behavior

2. **Product Analysis**
   - Category performance metrics
   - Product popularity rankings
   - Price point analysis
   - Bundle purchase patterns

3. **Order Processing**
   - Status transition analysis
   - Order value distribution
   - Items per order statistics
   - Category combinations

4. **Support Quality**
   - Resolution time by priority
   - Issue type distribution
   - Customer satisfaction patterns
   - Support load by customer segment

## Sample Data Overview

Current dataset includes:
- 7 customers with varying purchase patterns
- 6 products across 3 categories
- 12 orders with different statuses
- Multiple order items per order
- 12 customer interactions with various types and priorities

Value ranges:
- Order values: $89.98 to $1,899.97
- Product prices: $39.99 to $1,299.99
- Customer total spend: $699.97 to $4,639.84

## Notes for Development

1. **Connection Management**
   - Use connection pooling in production
   - Implement retry logic for connection failures
   - Monitor connection pool metrics

2. **Query Optimization**
   - Indexes are created on frequently queried columns
   - Complex queries should use CTEs for readability
   - Consider materialized views for heavy analytics

3. **Data Validation**
   - Check for NULL values in required fields
   - Validate foreign key relationships
   - Ensure data type consistency

4. **Error Handling**
   - Log all database errors
   - Implement proper error messages
   - Handle connection timeouts gracefully 