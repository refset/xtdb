# XTDB Temporal Columns in CDC Events

This document demonstrates how XTDB's temporal columns work in CDC events.

## Understanding Temporal Columns

XTDB uses **bitemporal** data management with two time dimensions:

- **Valid Time** (`_valid_from`, `_valid_to`): When the fact was true in the real world (business time)
- **System Time** (`_system_from`, `_system_to`): When the fact was recorded in the database (transaction time)

## Scenario 1: CREATE Operation

```json
{
  "_id": "user-123",
  "_valid_from": "2024-01-15T10:30:00Z",     // When this user data became valid
  "_valid_to": "9999-12-31T23:59:59.999999Z", // Still valid (max timestamp)
  "_system_from": "2024-01-15T10:30:00Z",     // When we recorded this in XTDB
  "_system_to": "9999-12-31T23:59:59.999999Z", // Still current (max timestamp)
  "name": "Alice Smith",
  "email": "alice@example.com"
}
```

**Explanation**: New record is valid and current from the transaction time until max time.

## Scenario 2: UPDATE Operation

When Alice changes her name, XTDB creates a new version and closes the old one:

### Before (Old Version)
```json
{
  "_id": "user-123",
  "_valid_from": "2024-01-15T10:30:00Z",     // When original became valid
  "_valid_to": "2024-01-15T14:30:00Z",       // When it stopped being valid ✨
  "_system_from": "2024-01-15T10:30:00Z",    // When we originally recorded it
  "_system_to": "2024-01-15T14:30:00Z",      // When it was superseded ✨
  "name": "Alice Smith",
  "email": "alice@example.com"
}
```

### After (New Version)
```json
{
  "_id": "user-123", 
  "_valid_from": "2024-01-15T14:30:00Z",     // When new data became valid ✨
  "_valid_to": "9999-12-31T23:59:59.999999Z", // Still valid
  "_system_from": "2024-01-15T14:30:00Z",    // When we recorded the update ✨
  "_system_to": "9999-12-31T23:59:59.999999Z", // Still current
  "name": "Alice Johnson-Smith",               // Updated name ✨
  "email": "alice.johnson@example.com",       // Updated email ✨
  "bio": "Software engineer"                  // New field ✨
}
```

**Explanation**: The old version gets closed at update time, new version starts at update time.

## Scenario 3: DELETE Operation

### Before (Deleted Version)
```json
{
  "_id": "user-999",
  "_valid_from": "2024-01-10T09:00:00Z",
  "_valid_to": "2024-01-15T16:00:00Z",       // Valid time ended at deletion ✨
  "_system_from": "2024-01-10T09:00:00Z", 
  "_system_to": "2024-01-15T16:00:00Z",      // System time ended at deletion ✨
  "name": "Bob Jones",
  "status": "inactive"
}
```

### After
```json
null  // No after state for deletions
```

**Explanation**: DELETE ends both valid and system time at deletion timestamp.

## Temporal Query Use Cases

These temporal columns enable powerful queries:

### 1. Current State (as of now)
```sql
SELECT * FROM users 
WHERE _system_to = '9999-12-31T23:59:59.999999'
```

### 2. Historical State (as of specific time)
```sql  
SELECT * FROM users
WHERE _system_from <= '2024-01-15T12:00:00Z' 
  AND _system_to > '2024-01-15T12:00:00Z'
```

### 3. Valid During Period
```sql
SELECT * FROM users  
WHERE _valid_from <= '2024-01-15T12:00:00Z'
  AND _valid_to > '2024-01-15T12:00:00Z'
```

### 4. Audit Trail (all versions)
```sql
SELECT _valid_from, _valid_to, _system_from, _system_to, name, email
FROM users 
WHERE _id = 'user-123'
ORDER BY _system_from
```

## CDC Consumer Benefits

CDC consumers can use these columns to:

1. **Reconstruct state at any point in time**
2. **Build temporal analytics** (when did changes happen?)
3. **Implement audit logging** (who changed what when?)
4. **Handle late-arriving data** (backdate corrections with valid time)
5. **Build slowly changing dimensions** (SCD Type 2) in data warehouses

## Example: Building a User Timeline

```python
def build_user_timeline(user_id):
    timeline = []
    for cdc_event in get_cdc_events_for_user(user_id):
        if cdc_event['op'] == 'c':
            timeline.append({
                'period': f"{cdc_event['after']['_valid_from']} to {cdc_event['after']['_valid_to']}",
                'name': cdc_event['after']['name'],
                'email': cdc_event['after']['email']
            })
        elif cdc_event['op'] == 'u':
            # Previous version period ended
            timeline.append({
                'period': f"{cdc_event['before']['_valid_from']} to {cdc_event['before']['_valid_to']}",  
                'name': cdc_event['before']['name'],
                'email': cdc_event['before']['email']
            })
            # New version period started
            timeline.append({
                'period': f"{cdc_event['after']['_valid_from']} to {cdc_event['after']['_valid_to']}",
                'name': cdc_event['after']['name'], 
                'email': cdc_event['after']['email']
            })
    return timeline
```

This would produce:
```
User user-123 Timeline:
- 2024-01-15T10:30:00Z to 2024-01-15T14:30:00Z: Alice Smith <alice@example.com>
- 2024-01-15T14:30:00Z to 9999-12-31T23:59:59.999999Z: Alice Johnson-Smith <alice.johnson@example.com>
```

## Integration with Data Pipelines

CDC consumers can easily build temporal data warehouses:

```sql
-- Slowly Changing Dimension Type 2
CREATE TABLE dim_user (
    user_id VARCHAR,
    name VARCHAR,
    email VARCHAR,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
);

-- Populated from CDC events
INSERT INTO dim_user 
SELECT 
    _id,
    name,
    email, 
    _valid_from,
    _valid_to,
    (_valid_to = '9999-12-31T23:59:59.999999') as is_current
FROM cdc_events;
```

This gives you full bitemporal capabilities in your downstream systems!