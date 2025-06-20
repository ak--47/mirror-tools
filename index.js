/**
 * @fileoverview this script demonstrates on mixpanel's mirror mode
 * can be used to build a data pipeline that transitions
 * from one day's identity graph to the next, while retroactively
 * associating events with identities.
 */

import { BigQuery } from "@google-cloud/bigquery";
import { ProjectsClient } from "@google-cloud/resource-manager";
import u from "ak-tools";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc.js";
dayjs.extend(utc);
import log from './logger.js';

let { NODE_ENV, DIRECTIVE = "" } = process.env;

const GCP_PROJECT_ID = "mixpanel-gtm-training";
const MAIN_DATASET = "mirror_mode_modeling_fun";
const MIRROR_SNAPSHOT_DATASET = "mirror_mode_snapshots";
const MIXPANEL_PROJECT_ID = "3739108";

const resourceClient = new ProjectsClient({ projectId: GCP_PROJECT_ID });
const bq = new BigQuery({ projectId: GCP_PROJECT_ID });

async function main(directive = "build") {
	if (DIRECTIVE) directive = DIRECTIVE.toLowerCase();
	log.info(`Running directive: ${directive}`);
	const startTime = dayjs.utc().subtract(7, "day");
	const sourceTables = generateTableData(startTime);
	const identities = generateIdentities(startTime);

	log.info("Checking datasets...");
	await ensureDataset(MAIN_DATASET);
	await ensureDataset(MIRROR_SNAPSHOT_DATASET);

	const serviceAccount = `serviceAccount:project-${MIXPANEL_PROJECT_ID}@mixpanel-warehouse-1.iam.gserviceaccount.com`;

	switch (directive) {
		case "build":
			log.info("Building tables from source data.");
			await policyBindings(serviceAccount, true);
			await buildTables(sourceTables, identities);
			await setCurrentIdentityGraph('yesterday');
			await materializeIdentityPermutations();
			break;

		case "transition-today":
			log.info("Transitioning to today's graph.");
			await setCurrentIdentityGraph('today');
			await materializeIdentityPermutations();
			break;

		case "transition-tomorrow":
			log.info("Transitioning to tomorrow's graph.");
			await setCurrentIdentityGraph('tomorrow');
			await materializeIdentityPermutations();
			break;

		case "transition-day-after-tomorrow":
			log.info("Transitioning to day after tomorrow's graph.");
			await setCurrentIdentityGraph('day_after_tomorrow');
			await materializeIdentityPermutations();
			break;

		case "delete":
			log.info("Deleting all tables and identities to start over.");
			await deleteAllTables();
			break;

		default:
			log.info("No directive provided, returning both table data and identities.");
			return { sourceTables, identities };
	}

	log.info("All operations completed successfully.");
	return;
}

function generateTableData(startDateObject) {
	const startTime = startDateObject.startOf('day'); // anchor at start of demo day
	const data = {
		tableDataWebsite: [
			{ event: "page view", anon_id: "foo", user_id: null, timestamp: startTime.add(8, "hour") }, // 8:00 AM
			{ event: "scroll", anon_id: "foo", timestamp: startTime.add(8, "hour").add(1, "m") },
			{ event: "click", anon_id: "foo", timestamp: startTime.add(8, "hour").add(2, "m") },
			{ event: "dropdown", anon_id: "foo", timestamp: startTime.add(8, "hour").add(3, "m") },
			
			// Later, logs in as bar
			{ event: "log in", user_id: "bar", timestamp: startTime.add(8, "hour").add(5, "m") },
			{ event: "doing stuff", user_id: "bar", timestamp: startTime.add(8, "hour").add(6, "m") },
			{ event: "doing more stuff", user_id: "bar", timestamp: startTime.add(8, "hour").add(7, "m") },
			{ event: "doing even more stuff", user_id: "bar", timestamp: startTime.add(8, "hour").add(8, "m") },
		],
		tableDataCRM: [
			{ event: "lead created", crm_user_id: "baz", timestamp: startTime.add(12, "hour") }, // Noon
			{ event: "campaign assigned", crm_user_id: "baz", timestamp: startTime.add(12, "hour").add(2, "m") },
		],
		tableDataServerLogs: [
			{ event: "server started", master_user_id: "qux", timestamp: startTime.add(18, "hour") }, // 6:00 PM
			{ event: "server stopped", master_user_id: "qux", timestamp: startTime.add(18, "hour").add(5, "m") },
		]
	};

	for (const t in data) data[t].forEach((row) => { 
		row.timestamp = row.timestamp.toISOString(); 
		if (row.anon_id) row.identity = row.anon_id;
		if (row.user_id) row.identity = row.user_id;
	});
	return data;
}

// Now each identity's first_seen exactly matches their first event:

function generateIdentities(startDateObject) {
	const startTime = startDateObject.startOf('day');

	return {
		identityGraphYesterday: [
			{
				cluster_id: "something_unique_123",
				as_of: startTime.subtract(1, "day").toISOString(),
				identities: [
					{
						identity: "foo",
						type: "anon_id",
						first_seen: startTime.add(8, "hour").toISOString(), // 8:00 AM
					}
				]
			}
		],
		identityGraphToday: [
			{
				cluster_id: "something_unique_123",
				as_of: startTime.toISOString(),
				identities: [
					{
						identity: "foo",
						type: "anon_id",
						first_seen: startTime.add(8, "hour").toISOString(), // 8:00 AM
					},
					{
						identity: "bar",
						type: "user_id",
						first_seen: startTime.add(8, "hour").add(5, "m").toISOString(), // log in event
					}
				]
			}
		],
		identityGraphTomorrow: [
			{
				cluster_id: "something_unique_123",
				as_of: startTime.add(1, "day").toISOString(),
				identities: [
					{
						identity: "foo",
						type: "anon_id",
						first_seen: startTime.add(8, "hour").toISOString(),
					},
					{
						identity: "bar",
						type: "user_id",
						first_seen: startTime.add(8, "hour").add(5, "m").toISOString(),
					},
					{
						identity: "baz",
						type: "crm_user_id",
						first_seen: startTime.add(12, "hour").toISOString(),
					}
				]
			}
		],
		identityGraphDayAfterTomorrow: [
			{
				cluster_id: "something_unique_123",
				as_of: startTime.add(2, "day").toISOString(),
				identities: [
					{
						identity: "foo",
						type: "anon_id",
						first_seen: startTime.add(8, "hour").toISOString(),
					},
					{
						identity: "bar",
						type: "user_id",
						first_seen: startTime.add(8, "hour").add(5, "m").toISOString(),
					},
					{
						identity: "baz",
						type: "crm_user_id",
						first_seen: startTime.add(12, "hour").toISOString(),
					},
					{
						identity: "qux",
						type: "master_user_id",
						first_seen: startTime.add(18, "hour").toISOString(),
					}
				]
			}
		]
	};
}

const identityClusterSchema = [
	{ name: "cluster_id", type: "STRING" },
	{ name: "as_of", type: "TIMESTAMP" },
	{
		name: "identities",
		type: "RECORD",
		mode: "REPEATED",
		fields: [
			{ name: "identity", type: "STRING" },
			{ name: "type", type: "STRING" },
			{ name: "first_seen", type: "TIMESTAMP" }
		]
	}
];

async function ensureDataset(name = MAIN_DATASET) {
	try {
		await bq.dataset(name).get({ autoCreate: true });
	} catch (e) {
		log.error("Failed to create or get dataset:", e);
		throw e;
	}
}

async function buildTables(sourceTables, identities) {
	const tablesToData = {
		website_data: sourceTables.tableDataWebsite,
		crm_data: sourceTables.tableDataCRM,
		server_logs: sourceTables.tableDataServerLogs,
		identities_yesterday: identities.identityGraphYesterday,
		identities_today: identities.identityGraphToday,
		identities_tomorrow: identities.identityGraphTomorrow,
		identities_day_after_tomorrow: identities.identityGraphDayAfterTomorrow,
	};

	for (const [tableName, rows] of Object.entries(tablesToData)) {
		if (!Array.isArray(rows) || rows.length === 0) {
			log.warn(`Skipping ${tableName}: no rows to insert!`);
			continue;
		}
		try {
			log.info(`Creating table ${tableName}...`);
			let schema = inferBQSchema(rows[0]);
			if (tableName.includes('identities')) schema = identityClusterSchema;
			const [tableObj] = await createOrReplaceTable(tableName, schema);
			await waitForTableToBeReady(tableObj);
			if (tableName.includes('identities')) {
				// DML INSERTS for identities tables
				for (const row of rows) {
					const sql = rowToInsertSQL(tableName, row);
					await bq.query({ query: sql, location: "US" });
				}
			} else {
				// Still use streaming for non-identity tables (or migrate if you want)
				const freshTable = bq.dataset(MAIN_DATASET).table(tableName);
				await waitForTableToBeReady(freshTable, 30, 500);
				await insertRows(freshTable, rows);
			}
			log.info(`✓ Table ${tableName} created and loaded successfully.`);
			await new Promise(res => setTimeout(res, 1000));
		} catch (error) {
			log.error(`Failed to create/load table ${tableName}:`, error);
			throw error;
		}
	}
}

async function createOrReplaceTable(table, schema) {
	const dataset = bq.dataset(MAIN_DATASET);
	const tableRef = dataset.table(table);

	try {
		const [exists] = await tableRef.exists();
		if (exists) {
			log.info(`Deleting existing table ${table}...`);
			await tableRef.delete();
			await new Promise(res => setTimeout(res, 1500));
		}
	} catch (e) {
		log.warn(`(Non-fatal) Error deleting table ${table}:`, e.message);
	}
	try {
		log.info(`Creating new table ${table}...`);
		const [tableObj] = await dataset.createTable(table, { schema });
		await new Promise(res => setTimeout(res, 1000));
		log.info(`Table ${table} created successfully`);
		return [tableObj];
	} catch (e) {
		log.error(`Error creating table ${table}:`, e);
		throw e;
	}
}

async function waitForTableToBeReady(table, retries = 20, maxInsertAttempts = 20) {
	log.info("Checking if table exits...");
	const tableName = table.id;
	tableExists: for (let i = 0; i < retries; i++) {
		const [exists] = await table.exists();
		if (exists) {
			log.info(`Table is confirmed to exist on attempt ${i + 1}.`);
			break tableExists;
		}
		const sleepTime = u.rand(1000, 5000);
		log.info(`Table sleeping for ${u.prettyTime(sleepTime)}; waiting for table exist; attempt ${i + 1}`);
		await u.sleep(sleepTime);
		if (i === retries - 1) {
			log.info(`Table does not exist after ${retries} attempts.`);
			return false;
		}
	}
	log.info("Checking if table is ready for operations...");
	let insertAttempt = 0;
	while (insertAttempt < maxInsertAttempts) {
		try {
			const dummyRecord = { [u.uid()]: u.uid() };
			const dummyInsertResult = await table.insert([dummyRecord]);
			log.info("...should never get here...");
			return true;
		} catch (error) {
			if (error.code === 404) {
				const sleepTime = u.rand(1000, 5000);
				log.info(`Table not ready for operations, sleeping ${u.prettyTime(sleepTime)} retrying... attempt #${insertAttempt + 1}`);
				await u.sleep(sleepTime);
				insertAttempt++;
			} else if (error.name === "PartialFailureError") {
				log.info("Table is ready for operations");
				return true;
			}
		}
	}
	return false;
}

async function insertRows(tableObj, rows) {
	if (!rows || rows.length === 0) {
		log.warn(`No rows to insert for table ${tableObj.id}`);
		return;
	}
	try {
		log.info(`Attempting to insert ${rows.length} rows into ${tableObj.id}`);
		const [exists] = await tableObj.exists();
		if (!exists) throw new Error(`Table ${tableObj.id} does not exist at insert time!`);
		const [metadata] = await tableObj.getMetadata();
		const fullTableName = `${metadata.tableReference.projectId}.${metadata.tableReference.datasetId}.${metadata.tableReference.tableId}`;
		log.info(`Full table reference: ${fullTableName}`);
		await tableObj.insert(rows);
		log.info(`Successfully inserted ${rows.length} rows into ${tableObj.id}`);
	} catch (err) {
		log.error(`Insert failed for ${tableObj.id}:`, {
			message: err.message,
			errors: err.errors,
			name: err.name,
			code: err.code
		});
		throw err;
	}
}

// --------- DML helpers for identity tables ----------

function rowToInsertSQL(table, row) {
	const identitiesArray = row.identities.map(id =>
		`STRUCT('${id.identity}', '${id.type}', TIMESTAMP('${id.first_seen}'))`
	).join(', ');
	return `
    INSERT INTO \`${bq.projectId}.${MAIN_DATASET}.${table}\` (cluster_id, as_of, identities)
    VALUES (
      '${row.cluster_id}',
      TIMESTAMP('${row.as_of}'),
      [${identitiesArray}]
    );
  `;
}

/**
 * Overwrite "current_identity_graph" with the contents of identities_{which} via DML.
 */
async function setCurrentIdentityGraph(which) {
	const valid = ['yesterday', 'today', 'tomorrow', 'day_after_tomorrow'];
	if (!valid.includes(which)) throw new Error(`Must be one of: ${valid.join(', ')}`);
	const projectId = bq.projectId;
	const datasetId = MAIN_DATASET;
	const srcTable = `\`${projectId}.${datasetId}.identities_${which}\``;
	const destTable = `\`${projectId}.${datasetId}.current_identity_graph\``;

	// 1. Ensure the destination table exists (create if missing)
	const schema = identityClusterSchema;
	const [destExists] = await bq.dataset(datasetId).table('current_identity_graph').exists();
	if (!destExists) {
		await bq.dataset(datasetId).createTable('current_identity_graph', { schema });
	}

	// 2. Delete all from destination (DML)
	await bq.query({ query: `DELETE FROM ${destTable} WHERE TRUE;`, location: "US" });

	// 3. Insert all from source (DML)
	await bq.query({ query: `INSERT INTO ${destTable} SELECT * FROM ${srcTable};`, location: "US" });

	log.info(`current_identity_graph replaced with identities_${which}`);
}

/**
 * Materialize all unique unordered pairs (or singles if only one id) in current_identity_graph.
 * Output: identity_permutations (schema: cluster_id STRING, ids ARRAY<STRING> (length 1 or 2))
 */
async function materializeIdentityPermutations() {
	const datasetId = MAIN_DATASET;
	const projectId = bq.projectId;
	const currentTable = `\`${projectId}.${datasetId}.current_identity_graph\``;
	const permTable = `\`${projectId}.${datasetId}.identity_permutations\``;

	const sql = `
    CREATE OR REPLACE TABLE ${permTable} AS
    WITH ids_with_idx AS (
      SELECT
        cluster_id,
        as_of,
        ARRAY(
          SELECT AS STRUCT id.identity, id.type, id.first_seen
          FROM UNNEST(identities) AS id
          ORDER BY id.first_seen, id.identity
        ) AS sorted_ids
      FROM ${currentTable}
    ),
    pairs AS (
      -- Chain pairs for N > 1
      SELECT
        cluster_id,
        as_of,
        ARRAY<STRING>[sorted_ids[OFFSET(i-1)].identity, sorted_ids[OFFSET(i)].identity] AS ids
      FROM ids_with_idx,
      UNNEST(GENERATE_ARRAY(1, ARRAY_LENGTH(sorted_ids)-1)) AS i
      WHERE ARRAY_LENGTH(sorted_ids) > 1

      UNION ALL

      -- Single identity cluster
      SELECT
        cluster_id,
        as_of,
        ARRAY<STRING>[sorted_ids[OFFSET(0)].identity] AS ids
      FROM ids_with_idx
      WHERE ARRAY_LENGTH(sorted_ids) = 1
    )
    SELECT
      cluster_id,
      as_of,
      ids,
      ids[OFFSET(0)] AS on_behalf_of,
      JSON_OBJECT('$distinct_ids', ids) AS distinct_ids_json
    FROM pairs
    ORDER BY cluster_id, as_of, ARRAY_TO_STRING(ids, ',')
  `;

	log.info("Materializing identity_permutations table...");
	await bq.query({ query: sql, location: "US" });
	log.info("✓ identity_permutations created/updated!");
}

function inferBQSchema(obj) {
	return Object.entries(obj).map(([name, value]) => ({
		name,
		type: inferBQType(value),
	}));
}
function inferBQType(val) {
	if (typeof val === "string") {
		if (/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(val)) return "TIMESTAMP";
		return "STRING";
	}
	if (typeof val === "number") return "FLOAT";
	if (typeof val === "boolean") return "BOOL";
	return "STRING";
}

async function deleteAllTables() {
	const [tables] = await bq.dataset(MAIN_DATASET).getTables();
	await Promise.all(tables.map(t => t.delete({ ignoreNotFound: true })));
	log.info(`All tables in ${MAIN_DATASET} deleted.`);
}

async function policyBindings(serviceAccount, add = true) {
	log.info(`assigning service account ${serviceAccount} to project ${GCP_PROJECT_ID}`);
	const roles = ["roles/bigquery.dataViewer", "roles/bigquery.jobUser"];
	const mirrorRoles = ["roles/bigquery.dataOwner"];
	const directive = add ? "Adding" : "Removing";

	//first do roles
	for (const role of roles) {
		try {
			// get policies
			const [policy] = await resourceClient.getIamPolicy({
				resource: resourceClient.projectPath(GCP_PROJECT_ID),
			});

			// Finds the binding in the policy
			let binding = policy.bindings.find((b) => b.role === role);

			// adds the user to the binding
			if (add) {
				if (!binding.members.includes(`${serviceAccount}`)) {
					binding.members.push(`${serviceAccount}`);
				}
			}

			// removes the user from the binding
			if (!add) {
				const memberIndex = binding.members.indexOf(`${serviceAccount}`);

				// If the member is found, remove it from the binding
				if (memberIndex > -1) {
					binding.members.splice(memberIndex, 1);

					// If no members left in this binding, remove the binding itself
					if (binding.members.length === 0) {
						const bindingIndex = policy[0].bindings.indexOf(binding);
						policy[0].bindings.splice(bindingIndex, 1);
					}
				}
			}

			// Sets the updated policy
			await resourceClient.setIamPolicy({
				resource: resourceClient.projectPath(GCP_PROJECT_ID),
				policy,
			});
			log.info(`${directive} user: ${serviceAccount} from ${GCP_PROJECT_ID} with role ${role}`);
		} catch (error) {
			log.error(`Error ${directive} ${serviceAccount} to ${role} :`, error);
			debugger;
		}
	}

	//then do dataOwner on schemas
	const query = `
CREATE SCHEMA IF NOT EXISTS \`${GCP_PROJECT_ID}\`.${MIRROR_SNAPSHOT_DATASET};

-- Grant mixpanel dataOwner permissions
GRANT \`roles/bigquery.dataOwner\`
  ON SCHEMA \`${GCP_PROJECT_ID}\`.${MIRROR_SNAPSHOT_DATASET}
  TO "${serviceAccount}";`;
	try {
		const result = await bq.query({ query });
		if (result[1]?.jobComplete) {
			log.info(`Schema ${MIRROR_SNAPSHOT_DATASET} created and permissions granted to ${serviceAccount}`);
		} else {
			log.error(`Failed to create schema or grant permissions: ${JSON.stringify(result)}`);
		}
		log.info(`Policy bindings for ${serviceAccount} updated successfully.`);
		return true;
	}
	catch (error) {
		result;
		debugger;
		return false;
	}
}

if (import.meta.url === new URL(`file://${process.argv[1]}`).href) {
	await main(DIRECTIVE || 'build');
	log.info("Script executed successfully.");
	if (NODE_ENV === "dev") debugger;
}

export default main;
