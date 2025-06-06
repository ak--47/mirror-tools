/**
 * @fileoverview this script demonstrates on mixpanel's mirror mode
 * can be used to build a data pipeline that transitions
 * from one day's identity graph to the next, while retroactively
 * associating events with identities.
 * 
 * basically when we first build we have two users with fragmented events
 * foo => bar  .... and baz stands alone
 * tomorrow, we want to transition the identity graph such that baz is merged into the cluster
 */



import { BigQuery } from "@google-cloud/bigquery";
import u from "ak-tools";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc.js";
dayjs.extend(utc);
import log from './logger.js';
const { NODE_ENV } = process.env;
const DATASET = "mirror_mode_fun";
const bq = new BigQuery({ projectId: "mixpanel-gtm-training" });


async function main(directive = "build") {
	const startTime = dayjs.utc().subtract(7, "day");
	const sourceTables = generateTableData(startTime);
	const identities = generateIdentities(startTime);

	switch (directive) {
		case "build":
			log.info("Building tables from source data.");
			await ensureDataset();
			await buildTables(sourceTables, identities);
			break;

		case "transition":
			log.info("Transitioning tables to the next day.");
			await ensureDataset();
			await transitionIdentityGraph();
			break;

		case "delete":
			log.info("Deleting all tables and identities to start over.");
			await deleteAllTables();
			break;

		default:
			log.info("No directive provided, returning both table data and identities.");
			return { sourceTables, identities };
	}

	return "done";
}

function generateTableData(startDateObject) {
	const startTime = startDateObject;
	const sourceTables = {
		tableDataWebsite: [
			{ event: "page view", anon_id: "foo", user_id: null, timestamp: startTime.subtract(10, "m") },
			{ event: "scroll", anon_id: "foo", user_id: null, timestamp: startTime.subtract(9, "m") },
			{ event: "click", anon_id: "foo", user_id: null, timestamp: startTime.subtract(8, "m") },
			{ event: "dropdown", anon_id: "foo", user_id: null, timestamp: startTime.subtract(7, "m") },
			{ event: "log in", user_id: "bar", user_id: null, timestamp: startTime.subtract(7, "m") },
			{ event: "doing stuff", user_id: "bar", anon_id: null, timestamp: startTime.subtract(6, "m") },
			{ event: "doing more stuff", user_id: "bar", anon_id: null, timestamp: startTime.subtract(5, "m") },
			{ event: "doing even more stuff", user_id: "bar", anon_id: null, timestamp: startTime.subtract(4, "m") },
		],
		tableDataERP: [
			{ event: "account provisioned", master_user_id: "baz", timestamp: startTime.subtract(3, "m") },
			{ event: "account alive", master_user_id: "baz", timestamp: startTime.subtract(2, "m") },
		],
		tableDataServerLogs: [
			{ event: "server started", master_user_id: "baz", timestamp: startTime.subtract(1, "m") },
			{ event: "server stopped", master_user_id: "baz", timestamp: startTime.subtract(30, "s") },
		]
	};
	for (const t in sourceTables) sourceTables[t].forEach((row) => { row.timestamp = row.timestamp.toISOString(); });
	return sourceTables;
}

function generateIdentities(startDateObject) {
	const startTime = startDateObject;

	return {
		identityGraphToday: [
			{
				cluster_id: "foo_bar",
				as_of: startTime.format("YYYY-MM-DD"),
				identities: [
					{
						identity: "foo",
						type: "anon_id",
						first_seen: startTime.subtract(10, "m").toISOString(),
					},
					{
						identity: "bar",
						type: "user_id",
						first_seen: startTime.subtract(7, "m").toISOString(),
					}
				]
			}
		],
		identityGraphTomorrow: [
			{
				cluster_id: "foo_bar_baz",
				as_of: startTime.add(1, "day").format("YYYY-MM-DD"),
				identities: [
					{
						identity: "foo",
						type: "anon_id",
						first_seen: startTime.subtract(10, "m").toISOString(),
					},
					{
						identity: "bar",
						type: "user_id",
						first_seen: startTime.subtract(7, "m").toISOString(),
					},
					{
						identity: "baz",
						type: "master_user_id",
						first_seen: startTime.subtract(3, "m").toISOString(),
					}
				]
			}
		]
	};
}

const identityClusterSchema = [
	{ name: "cluster_id", type: "STRING" },
	{ name: "as_of", type: "DATE" },
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


async function ensureDataset() {
	try {
		await bq.dataset(DATASET).get({ autoCreate: true });
	} catch (e) {
		log.error("Failed to create or get dataset:", e);
		throw e;
	}
}

async function buildTables(sourceTables, identities) {
	const tablesToData = {
		website_data: sourceTables.tableDataWebsite,
		erp_data: sourceTables.tableDataERP,
		server_logs: sourceTables.tableDataServerLogs,
		identities_today: identities.identityGraphToday,
		identities_tomorrow: identities.identityGraphTomorrow,
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
			log.info(`Table ${tableName} created successfully.`);

			log.info(`Waiting for table ${tableName} to be ready...`);
			await waitForTableToBeReady(tableObj);

			// KEY FIX: Get a fresh reference to the table before inserting
			log.info(`Getting fresh table reference for ${tableName}...`);
			const freshTable = bq.dataset(DATASET).table(tableName);

			// Verify the fresh table exists and is ready
			await waitForTableToBeReady(freshTable, 30, 500);

			log.info(`Inserting ${rows.length} rows into ${tableName}...`);
			await insertRows(freshTable, rows);

			log.info(`✓ Table ${tableName} created and loaded successfully.`);

			// Small delay between tables
			await new Promise(res => setTimeout(res, 1000));

		} catch (error) {
			log.error(`Failed to create/load table ${tableName}:`, error);
			throw error;
		}
	}
}

// Enhanced createOrReplaceTable with better error handling
async function createOrReplaceTable(table, schema) {
	const dataset = bq.dataset(DATASET);
	const tableRef = dataset.table(table);

	try {
		// First, ensure we fully delete any existing table
		const [exists] = await tableRef.exists();
		if (exists) {
			log.info(`Deleting existing table ${table}...`);
			await tableRef.delete();
			// Wait a bit after deletion
			await new Promise(res => setTimeout(res, 1500));
		}
	} catch (e) {
		log.warn(`(Non-fatal) Error deleting table ${table}:`, e.message);
	}

	try {
		log.info(`Creating new table ${table}...`);
		const [tableObj] = await dataset.createTable(table, { schema });

		// Wait a moment after creation
		await new Promise(res => setTimeout(res, 1000));

		log.info(`Table ${table} created successfully`);
		return [tableObj];
	} catch (e) {
		log.error(`Error creating table ${table}:`, e);
		throw e;
	}
}

// More robust table readiness check
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
			// Attempt a dummy insert that SHOULD fail, but not because 404
			const dummyRecord = { [u.uid()]: u.uid() };
			const dummyInsertResult = await table.insert([dummyRecord]);
			log.info("...should never get here...");
			return true; // If successful, return true immediately
		} catch (error) {
			if (error.code === 404) {
				const sleepTime = u.rand(1000, 5000);
				log.info(`Table not ready for operations, sleeping ${u.prettyTime(sleepTime)} retrying... attempt #${insertAttempt + 1}`);
				await u.sleep(sleepTime);
				insertAttempt++;
			} else if (error.name === "PartialFailureError") {
				log.info("Table is ready for operations");
				return true;
			} else {
				log.info("should never get here either");
				if (NODE_ENV === 'test') debugger;
			}
		}
	}
	return false; // Return false if all attempts fail
}

// Enhanced insertRows with better debugging and retry logic
async function insertRows(tableObj, rows) {
	if (!rows || rows.length === 0) {
		log.warn(`No rows to insert for table ${tableObj.id}`);
		return;
	}

	try {
		log.info(`Attempting to insert ${rows.length} rows into ${tableObj.id}`);

		// Double-check the table exists right before insert
		const [exists] = await tableObj.exists();
		if (!exists) {
			throw new Error(`Table ${tableObj.id} does not exist at insert time!`);
		}

		// Log the full table reference for debugging
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

		// If it's a "not found" error, try one more time with a fresh reference
		if (err.message && (err.message.includes('not found') || err.message.includes('does not exist'))) {
			log.warn(`Table not found error - trying with fresh reference...`);

			// Get the table name from the current reference
			const tableName = tableObj.id;
			const freshTable = bq.dataset(DATASET).table(tableName);

			try {
				await waitForTableToBeReady(freshTable, 20, 2000);
				await freshTable.insert(rows);
				log.info(`Retry with fresh reference succeeded for ${tableName}`);
				return;
			} catch (retryErr) {
				log.error(`Retry also failed for ${tableName}:`, retryErr);
			}
		}

		throw err;
	}
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

async function transitionIdentityGraph() {
	const dataset = bq.dataset(DATASET);
	const todayTable = dataset.table('identityGraphToday');
	const tomorrowTable = dataset.table('identityGraphTomorrow');

	await todayTable.delete({ ignoreNotFound: true });
	const [metadata] = await tomorrowTable.getMetadata();
	const [newTodayObj] = await dataset.createTable('identityGraphToday', { schema: metadata.schema });
	await waitForTableToBeReady(newTodayObj);

	const [rows] = await tomorrowTable.getRows();
	if (rows.length > 0) await newTodayObj.insert(rows);
	log.info('identityGraphToday replaced with identityGraphTomorrow');
}

async function deleteAllTables() {
	const [tables] = await bq.dataset(DATASET).getTables();
	await Promise.all(tables.map(t => t.delete({ ignoreNotFound: true })));
	log.info(`All tables in ${DATASET} deleted.`);
}

if (import.meta.url === new URL(`file://${process.argv[1]}`).href) {
	const results = await main('build');
	if (NODE_ENV === "dev") debugger;
}
