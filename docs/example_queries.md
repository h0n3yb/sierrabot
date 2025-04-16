# Example Natural Language Queries

This document provides examples of natural language queries that our autonomous transformation agent can process. Each query demonstrates how users can express their data transformation needs in everyday language, which the agent then converts into optimized Spark code.

## Basic Analysis Queries

### Customer Insights
```plaintext
"Show me our highest spending customers and what categories they typically buy from"
```
*Demonstrates*: Multi-table joins, aggregation, and category analysis
*Use Case*: Customer segmentation and purchasing behavior analysis

```plaintext
"Calculate the average time it takes to resolve customer support tickets by priority level"
```
*Demonstrates*: Time-based calculations, conditional grouping
*Use Case*: Support quality monitoring

### Product Performance
```plaintext
"Which product categories generate the most revenue from completed orders?"
```
*Demonstrates*: Sales analysis, status filtering
*Use Case*: Category performance evaluation

```plaintext
"Find products that are frequently purchased together in the same order"
```
*Demonstrates*: Self-joins, pattern recognition
*Use Case*: Bundle recommendations

## Advanced Analysis

### Customer Behavior
```plaintext
"Create a customer engagement score based on their purchase frequency, total spend, and support interaction history"
```
*Demonstrates*: Complex scoring logic, multiple metric combination
*Use Case*: Customer segmentation

```plaintext
"Identify customers who have switched between product categories in their recent orders compared to their first purchase"
```
*Demonstrates*: Time-series analysis, behavior change detection
*Use Case*: Customer journey analysis

### Support Quality Metrics
```plaintext
"Calculate the percentage of support tickets that were resolved within 24 hours, broken down by priority and type"
```
*Demonstrates*: Time window analysis, percentage calculations
*Use Case*: SLA compliance monitoring

### Cross-Category Analysis
```plaintext
"Show me which customers tend to buy products across multiple categories in a single order"
```
*Demonstrates*: Pattern recognition, basket analysis
*Use Case*: Cross-selling opportunities

## Business Intelligence

### Trend Analysis
```plaintext
"Compare the average order value and frequency between new and returning customers"
```
*Demonstrates*: Customer segmentation, comparative analysis
*Use Case*: Customer lifecycle analysis

```plaintext
"Show the trend of support ticket resolution times over the past month by priority level"
```
*Demonstrates*: Time-series analysis, trend detection
*Use Case*: Support team performance monitoring

### Performance Metrics
```plaintext
"Calculate the customer retention rate based on repeat purchases within 30 days"
```
*Demonstrates*: Cohort analysis, time-window calculations
*Use Case*: Retention metrics

## Complex Scenarios

### Multi-Metric Analysis
```plaintext
"Create a customer health score that combines purchase frequency, support ticket resolution satisfaction, and total spend, weighted by recency"
```
*Demonstrates*: Complex scoring, multiple metric combination, time decay
*Use Case*: Customer health monitoring

### Predictive Indicators
```plaintext
"Identify patterns in support tickets that typically precede customer churn"
```
*Demonstrates*: Pattern recognition, sequence analysis
*Use Case*: Churn prediction

## Data Quality Checks

### Validation Queries
```plaintext
"Find any orders where the sum of order items doesn't match the order total"
```
*Demonstrates*: Data consistency checking
*Use Case*: Data quality assurance

```plaintext
"Show me any customers with support tickets but no purchase history"
```
*Demonstrates*: Anomaly detection
*Use Case*: Data integrity verification

## Employee Analytics

### Basic Employee Insights
```plaintext
"Show me the average salary progression of employees over their first 5 years in each department"
```
*Demonstrates*: Time-series analysis, department-wise aggregation
*Use Case*: Career progression analysis

```plaintext
"Find departments with the highest employee retention rates"
```
*Demonstrates*: Employee tenure calculation, departmental comparison
*Use Case*: Department performance evaluation

### Management Structure
```plaintext
"List departments where managers have been promoted from within the department"
```
*Demonstrates*: Career path tracking, internal mobility analysis
*Use Case*: Succession planning

```plaintext
"Calculate the average time employees spend under each manager before getting promoted"
```
*Demonstrates*: Management effectiveness, promotion patterns
*Use Case*: Leadership impact assessment

## Advanced Employee Analysis

### Compensation Patterns
```plaintext
"Identify salary trends and disparities across different departments, accounting for employee tenure and title"
```
*Demonstrates*: Multi-factor analysis, equity assessment
*Use Case*: Compensation fairness review

```plaintext
"Compare the salary growth rates between employees who changed departments versus those who stayed"
```
*Demonstrates*: Comparative analysis, career path impact
*Use Case*: Internal mobility impact assessment

### Career Progression
```plaintext
"Show the most common career paths that lead to senior management positions"
```
*Demonstrates*: Career trajectory analysis, path optimization
*Use Case*: Career development planning

### Department Dynamics
```plaintext
"Calculate the ratio of internal promotions to external hires for management positions by department"
```
*Demonstrates*: Hiring pattern analysis, promotional practices
*Use Case*: Talent development assessment

## Organizational Intelligence

### Workforce Planning
```plaintext
"Show departments with the highest turnover rates in senior positions over the last 3 years"
```
*Demonstrates*: Leadership stability analysis, time-window calculations
*Use Case*: Succession risk assessment

```plaintext
"Identify departments where the average time to promotion is significantly longer than the company average"
```
*Demonstrates*: Comparative analysis, organizational bottlenecks
*Use Case*: Career mobility optimization

### Performance Metrics
```plaintext
"Calculate the correlation between department size and average employee tenure"
```
*Demonstrates*: Organizational dynamics, correlation analysis
*Use Case*: Department structure optimization

## Complex Scenarios

### Multi-Dimensional Analysis
```plaintext
"Create an employee engagement index based on tenure, promotion frequency, and salary growth rate, weighted by department performance"
```
*Demonstrates*: Complex scoring, multiple metric combination
*Use Case*: Employee satisfaction assessment

### Predictive Indicators
```plaintext
"Identify common career progression patterns that typically lead to long-term retention"
```
*Demonstrates*: Pattern recognition, retention analysis
*Use Case*: Career path optimization

## Data Quality Checks

### Validation Queries
```plaintext
"Find any inconsistencies between employee titles and their salary ranges within departments"
```
*Demonstrates*: Data consistency validation
*Use Case*: Compensation structure verification

```plaintext
"Show any gaps or overlaps in employee management history"
```
*Demonstrates*: Timeline consistency checking
*Use Case*: Management history validation

## Implementation Notes

1. **Query Processing**
   - Agent translates natural language to specific transformation types
   - Identifies relevant tables and relationships
   - Determines required metrics and calculations
   - Generates optimized Spark code with proper validation

2. **Execution Flow**
   - Validates query understanding
   - Generates initial code
   - Performs test execution
   - Self-corrects if needed
   - Confirms results with user

3. **Best Practices**
   - Use clear, everyday language
   - Include time periods when relevant
   - Specify any thresholds or conditions
   - Mention desired metrics clearly

4. **Common Patterns**
   - Aggregation requests
   - Time-based analysis
   - Cross-table relationships
   - Pattern detection
   - Metric calculations

## Alignment with Requirements

These example queries demonstrate the agent's ability to:
1. Accept natural language transformation instructions
2. Generate appropriate Spark code
3. Execute against read-only PostgreSQL
4. Validate results
5. Support both simple and complex transformations
6. Handle various business scenarios

The queries are designed to:
- Be deterministic in execution
- Support clear responsibility separation
- Enable autonomous operation
- Provide clear audit trails
- Maintain data security 

These examples demonstrate additional capabilities of the agent to:
1. Handle complex organizational analytics
2. Process time-based career progression analysis
3. Identify patterns in employee development
4. Validate organizational data integrity
5. Support workforce planning decisions
6. Analyze compensation equity

The queries maintain:
- Deterministic execution patterns
- Clear responsibility separation
- Autonomous operation capability
- Comprehensive audit trails
- Data security compliance 