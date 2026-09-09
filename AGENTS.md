# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This project contains two independent demonstrations of Mixpanel's **mirror mode** capabilities:

1. **Identity Graph Demo** (`identities.js`) - Shows how identity clusters evolve over time (across 4 days) and demonstrates retroactive event association as identities merge
2. **Transaction Schema Evolution Demo** (`transactions.js`) - Demonstrates complex schema evolution including inserts, updates, deletes, and new columns across time periods

Both demos show how BigQuery data pipelines can model state transitions that Mixpanel's mirror mode can track and snapshot.

## Common Commands

### Identity Graph Demo (identities.js)
```bash
# Initial build - creates all tables and sets up initial identity state
npm run id:build

# Transition identity graph to different time periods
npm run id:trans:today
npm run id:trans:tomorrow  
npm run id:trans:day-after-tomorrow

# Clean up - deletes all identity tables and data
npm run id:delete

# Reset - equivalent to delete + build
npm run id:reset
```

### Transaction Schema Evolution Demo (transactions.js)
```bash
# Initial build - creates transaction tables for yesterday/today
npm run tx:build

# Transition to today's transaction state (shows schema evolution)
npm run tx:trans:today

# Clean up - deletes all transaction tables and data
npm run tx:delete

# Reset - equivalent to delete + build
npm run tx:reset
```

### Direct Script Execution
```bash
# Identity demo alternatives using DIRECTIVE environment variable
DIRECTIVE=build node identities.js
DIRECTIVE=transition-today node identities.js
DIRECTIVE=delete node identities.js

# Transaction demo alternatives using DIRECTIVE environment variable
DIRECTIVE=build node transactions.js
DIRECTIVE=transition-today node transactions.js
DIRECTIVE=delete node transactions.js
```

### Event Sequencing Demo
```bash
# Standalone script that sends events to Mixpanel
node event-sequenencing.js
```

## Architecture

### Identity Graph Demo (`identities.js`)
- **BigQuery Integration**: Creates and manages datasets, tables, and identity clusters
- **Identity Graph Evolution**: Simulates 4 time periods with progressively merged identities:
  - Yesterday: 1 identity (`foo`)
  - Today: 2 identities (`foo`, `bar`) 
  - Tomorrow: 3 identities (`foo`, `bar`, `baz`)
  - Day After Tomorrow: 4 identities (`foo`, `bar`, `baz`, `qux`)
- **DML-based Transitions**: Uses BigQuery DML to update `current_identity_graph` table rather than replacing entire tables
- **Identity Permutation Materialization**: Generates minimal N-1 pairs to connect identity graphs
- **Dataset**: `mirror_mode_modeling_fun`

#### Identity Data Structure
- **Source Tables**: `website_data`, `crm_data`, `server_logs` with realistic event sequences
- **Identity Tables**: Separate tables for each time period (`identities_yesterday`, etc.)
- **Current State**: `current_identity_graph` - active identity cluster
- **Permutations**: `identity_permutations` - materialized identity pairs for graph connectivity

### Transaction Schema Evolution Demo (`transactions.js`)
- **Schema Evolution Simulation**: Demonstrates complex data changes across 2 time periods
- **Yesterday State**: 10 transactions, all `finalized: false`
- **Today State**: 14 transactions with schema evolution:
  - 9 surviving transactions from yesterday (1 deleted for GDPR simulation)
  - All yesterday transactions now have `finalized: true`
  - New column `is_suspected_fraud` added to all rows
  - 5 new transactions added for today
- **DML-based Transitions**: Uses BigQuery DML to update `current_transactions` table with schema changes
- **Dataset**: `mirror_transactions_demo`

#### Transaction Data Structure
- **Transaction Tables**: `transactions_yesterday`, `transactions_today` with different schemas
- **Current State**: `current_transactions` - active transaction dataset
- **Schema Changes**: Handles column additions, row deletions, and value updates across transitions

### Configuration
- **GCP Project**: `mixpanel-gtm-training`
- **Identity Dataset**: `mirror_mode_modeling_fun` 
- **Transaction Dataset**: `mirror_transactions_demo`
- **Snapshot Datasets**: `mirror_mode_snapshots`, `mirror_transactions_snapshots`
- **Mixpanel Project**: `3739108`

### Dependencies
- **BigQuery**: Main data warehouse operations
- **Google Cloud Resource Manager**: IAM policy management  
- **ak-tools**: Utility functions for timing, UIDs, etc.
- **dayjs**: Date/time manipulation with UTC plugin
- **pino**: Structured logging with pretty printing in dev mode
- **mixpanel-import**: For sending events to Mixpanel (used in event-sequenencing.js)

## Development Notes

### General
- Both demos include automatic IAM setup for Mixpanel service accounts
- All operations use DML for transitions to ensure Mixpanel can capture state changes via snapshots
- Includes retry logic and error handling for BigQuery operations
- VSCode launch configurations available for debugging different directives for both demos

### Identity Demo Specifics
- All timestamps are carefully aligned so identity `first_seen` matches actual event timestamps
- Identity permutation materialization uses minimal N-1 pairings to connect identity graphs

### Transaction Demo Specifics
- Demonstrates realistic e-commerce transaction data with multiple payment methods and product categories
- Schema evolution includes both structural changes (new columns) and data changes (updates, deletes)
- GDPR simulation shows how data deletion is handled in mirror mode
- Fraud detection column addition demonstrates how new business logic can be retroactively applied