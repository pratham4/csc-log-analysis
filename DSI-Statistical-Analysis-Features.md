# DSI Transaction Statistical Analysis Features

This document describes the new statistical chat message capabilities for analyzing DSI transaction logs (dsitransactionlog and dsitransactionlogarchive).

## New Statistical Chat Messages

### 1. Most Occurring Errors
**Queries patterns:**
- "Which are most occurring errors in last five days"
- "Most common errors in last week"
- "Most frequent errors for instance ABC123"
- "Show most occurring errors in last month"

**Example usage:**
```
User: "Which are most occurring errors in last five days?"
User: "Most common errors for instance DEV001 in last week"
```

**Response includes:**
- Error messages with occurrence counts
- Affected instances
- Time period analyzed
- Error previews (truncated for readability)

### 2. Instance-Specific Error Analysis
**Query patterns:**
- "List the errors those occurred yesterday for instance {id}"
- "Show errors for instance XYZ789 yesterday"
- "Errors on 2024-11-20 for instance ABC123"

**Example usage:**
```
User: "List the errors those occurred yesterday for instance DEV001"
User: "Show errors for instance PROD-SERVER-01 on 2024-11-19"
```

**Response includes:**
- All errors for the specific instance and date
- Error timestamps
- Function call details
- Error descriptions
- User IDs who triggered the errors

### 3. Logs Around Error Time
**Query patterns:**
- "Which were the logs around a minute of a particular error for an instance {id}"
- "Show logs around error time 20241120145300 for instance DEV001"
- "Logs around minute of error for instance ABC123"

**Example usage:**
```
User: "Show logs around minute of error 20241120145300 for instance DEV001"
User: "Logs around error time for instance PROD-01" (requires error time to be specified)
```

**Response includes:**
- All transaction logs within the time window (default: 1 minute before/after)
- Both successful and failed transactions
- User activity during the error period
- Function calls and their results

### 4. Users with Most Errors
**Query patterns:**
- "Which user caused most errors in last 5 days for instance {id}"
- "Users with most errors for instance ABC123"
- "Top users by error count for instance DEV001 last week"

**Example usage:**
```
User: "Which user caused most errors in last 5 days for instance DEV001?"
User: "Show users with most errors for instance PROD-SERVER-01 in last month"
```

**Response includes:**
- User IDs ranked by error count
- Error counts per user
- Time period analyzed
- Instance filter applied

### 5. Logs Around Specific DateTime
**Query patterns:**
- "Give me all logs around 2 mins of {date time} for instance {id}"
- "Give me all logs around 2 mins of {date time} for instance {id} and user {id}"
- "Show logs around 2024-11-20T14:30:00 for instance DEV001"

**Example usage:**
```
User: "Give me all logs around 2 mins of 2024-11-20T14:30:00 for instance DEV001"
User: "Show logs around 2024-11-20 14:30:00 for instance PROD-01 and user john"
```

**Response includes:**
- All logs within 2 minutes (default) of the specified time
- Optional user filtering
- Transaction details including data in/out (truncated)
- Success/error status for each log

### 6. Multi-Criteria Log Filtering
**Query patterns:**
- "Filter logs specific to instance, user, Agent"
- "Show logs for instance ABC123 user john last week"
- "Filter logs by instance DEV001 with errors only"
- "Show DSI logs for user admin last 3 days"

**Example usage:**
```
User: "Filter logs for instance DEV001 user john last week"
User: "Show logs with errors only for instance PROD-01"
User: "Filter DSI logs by user admin last 3 days"
```

**Response includes:**
- Logs matching all specified criteria
- Configurable time periods
- Option to show only errors or all logs
- Support for instance, user, and application ID filters

## Technical Implementation

### New MCP Tools Added
1. `get_most_occurring_errors` - Analyzes error frequency across time periods
2. `get_errors_for_instance_date` - Gets all errors for specific instance/date
3. `get_logs_around_error_time` - Retrieves logs in time window around error
4. `get_users_with_most_errors` - Ranks users by error count for instance
5. `get_logs_around_datetime` - Gets logs around specific datetime with user filter
6. `get_filtered_dsi_logs` - Multi-criteria log filtering

### New Service
- `DSIStatsService` - Core statistical analysis service
- Handles both main and archive table queries
- Intelligent date parsing and filtering
- Performance optimized with query limits

### Chat Integration
- Enhanced LLM pattern recognition for statistical queries
- Automatic parameter extraction (instance IDs, dates, users)
- Intelligent fallback handling for missing parameters
- Rich formatted responses with structured content

## Usage Examples

### Basic Error Analysis
```
User: "Most occurring errors in last 5 days"
Response: Shows top 10 errors across all instances with occurrence counts

User: "Most occurring errors for instance DEV001"
Response: Shows top 10 errors specific to DEV001 instance
```

### Time-Based Analysis
```
User: "Errors yesterday for instance PROD-SERVER-01"
Response: All errors from yesterday for that specific instance

User: "Logs around 2024-11-20T14:30:00 for instance DEV001"
Response: All logs within 2 minutes of that timestamp
```

### User Analysis
```
User: "Which user caused most errors for instance DEV001 last week?"
Response: Ranked list of users by error count for that instance
```

### Advanced Filtering
```
User: "Filter logs for instance DEV001 user john with errors only last 3 days"
Response: Only error logs for that instance/user combination in the specified period
```

## Database Tables Used

- **dsitransactionlog** - Current transaction logs
- **dsitransactionlogarchive** - Archived transaction logs

Both tables are automatically queried and results are combined for comprehensive analysis.

## Key Features

1. **Intelligent Parameter Extraction** - Automatically extracts instance IDs, dates, and user IDs from natural language
2. **Flexible Time Periods** - Supports various time expressions ("last 5 days", "yesterday", "last week", etc.)
3. **Cross-Table Queries** - Searches both main and archive tables automatically
4. **Performance Optimized** - Query limits and intelligent indexing
5. **Rich Responses** - Formatted output with key statistics and details
6. **Error Handling** - Graceful handling of missing parameters with helpful error messages

## Integration Points

- **Chat Service** - Pattern recognition and response formatting
- **LLM Service** - Enhanced natural language understanding
- **MCP Server** - Core statistical computation tools
- **Database Models** - Direct integration with transaction log models

This implementation provides comprehensive statistical analysis capabilities for DSI transaction logs while maintaining the existing chat interface patterns and user experience.