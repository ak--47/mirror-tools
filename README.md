# Mirror Mode Identity Graph Demo

This project demonstrates Mixpanel's **mirror mode**—specifically, how emergent identity graphs can change over time and be modeled in data pipelines using BigQuery.

By simulating "transition states" of an identity cluster, it illustrates how Mixpanel’s [**original identity merge**](https://docs.mixpanel.com/docs/tracking-methods/id-management#identity-merge-apis) provides APIs to interact with evolving identity clusters, proving how retroactive association of events works across sources. This repo is both an educational playground and a reference for real-world identity stitching pipelines.

---

## TL;DR

* **Simulates real-world event tables** (`website_data`, `crm_data`, `server_logs`) with overlapping and fragmented identities.
* **Tracks identity clusters across four days:**

  * *Yesterday*: single identity
  * *Today*: two merged identities
  * *Tomorrow*: three merged identities
  * *Day After Tomorrow*: four merged identities (final state)
* **DML-based transition**: Each day’s identity cluster state replaces the previous day’s via DML, not by replacing tables—making it snapshot-friendly for downstream consumers.
* **Materializes minimal identity permutation pairs** for each cluster: for N identities, produces N-1 pairs to minimally connect the graph.
* **Full local automation**: All setup, transition, and teardown tasks are orchestrated by the script and can be run via npm/yarn scripts or VSCode launch configs.

---

## Setup

### 1. Prerequisites

* Node.js v18+
* Google Cloud SDK and credentials with BigQuery permissions
* A GCP project with BigQuery enabled

### 2. Install dependencies

```sh
npm install
```

### 3. Environment

Set `GCP_PROJECT_ID` and any other relevant env vars (or edit directly in `index.js`).

---

## Usage

### CLI scripts (see `package.json`):

* **Initial Build:**

  ```sh
  npm run build
  # or
  DIRECTIVE=build node index.js
  ```

  Loads all data tables and uploads four identity cluster tables (yesterday, today, tomorrow, day after tomorrow).

* **Transition to Today:**

  ```sh
  npm run trans:today
  # or
  DIRECTIVE=transition-today node index.js
  ```

  Replaces the current identity graph with today’s cluster and materializes permutations.

* **Transition to Tomorrow:**

  ```sh
  npm run trans:tomorrow
  # or
  DIRECTIVE=transition-tomorrow node index.js
  ```

  Moves to the next cluster state (three merged) and updates permutations.

* **Transition to Day After Tomorrow:**

  ```sh
  npm run trans:day-after-tomorrow
  # or
  DIRECTIVE=transition-day-after-tomorrow node index.js
  ```

  Moves to the final state (four merged IDs).

* **Delete everything:**

  ```sh
  npm run delete
  # or
  DIRECTIVE=delete node index.js
  ```

  Drops all tables in the working dataset.

---

## Table Design

### Main Tables

* **website\_data**: events with `anon_id` and/or `user_id`
* **crm\_data, server\_logs**: events with `crm_user_id` and `master_user_id`
* **current\_identity\_graph**: The single-row identity cluster in play for all identity permutations.
* **identity\_permutations**: Materialized pairs from `current_identity_graph`—for N identities, produces N-1 rows, each as an array of max 2 ids.

### Example Output

#### identity\_permutations

| cluster\_id            | ids            |
| ---------------------- | -------------- |
| something\_unique\_123 | \["foo"]       |
| something\_unique\_123 | \["foo","bar"] |
| something\_unique\_123 | \["bar","baz"] |
| something\_unique\_123 | \["baz","qux"] |

#### Transition Behavior

* On build: only `["foo"]`
* After transition-today: `["foo","bar"]`
* After transition-tomorrow: `["foo","bar"]`, `["bar","baz"]`
* After transition-dayafter: `["foo","bar"]`, `["bar","baz"]`, `["baz","qux"]`

---

## How It Works

1. **Build**: Loads sample data and four days of identity cluster states as separate tables. Sets up the current cluster.
2. **Transition**: Uses DML to update `current_identity_graph` with the target day’s identities—preserving the table for easy diffing/snapshotting by external services.
3. **Materialization**: For the active cluster, generates exactly N-1 pairings by chaining sorted identities, always outputting minimal pairs to “connect the graph.”
4. **IAM Setup**: Optionally grants BigQuery roles to a configured service account for demo/experimentation.

---

## Realistic Sample Data

* Timestamps are aligned so that `first_seen` for each identity matches their actual first event in the sample data tables.
* Event data simulates a plausible onboarding/merge sequence across digital touchpoints (web, CRM, backend).

---

## VSCode Debugging

The `.vscode/launch.json` is pre-configured for direct invocation of build, transition, and delete operations.

---

## License

MIT or public domain—do whatever you want!
