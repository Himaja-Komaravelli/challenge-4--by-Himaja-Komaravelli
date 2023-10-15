import * as fs from 'fs';
import * as path from 'path';
import * as https from 'https';
import * as zlib from 'zlib';
import * as tar from 'tar';
import knex from 'knex'; // Import KnexTimeoutError
import { DUMP_DOWNLOAD_URL } from './resources';
import { parse } from 'csv-parse';

export async function processDataDump() {
  try {
    // Define the URL to download data, and the directories for temporary and output files.
    const downloadUrl = DUMP_DOWNLOAD_URL;
    const tmpDir = path.join("../challenge-1", 'tmp');
    const outDir = path.join("../challenge-1", 'out');

    // Ensure that the temporary and output directories exist.
    fs.mkdirSync(tmpDir, { recursive: true });
    fs.mkdirSync(outDir, { recursive: true });

    // Define a function to download and extract a tar.gz file.
    async function downloadAndExtract() {
      try {
        const tarballPath = path.join(tmpDir, 'dump.tar.gz');
        const extractPath = path.join(tmpDir, 'dump.tar');

        const fileStream = fs.createWriteStream(tarballPath);
        await new Promise<void>((resolve, reject) => {
          https.get(downloadUrl, (response) => {
            response.pipe(fileStream);
            fileStream.on('finish', () => {
              fileStream.close();
              // Extract the tarball
              fs.mkdirSync(extractPath);
              fs.createReadStream(tarballPath)
                .pipe(zlib.createGunzip())
                .pipe(tar.extract({ cwd: extractPath }))
                .on('end', () => {
                  resolve();
                });
            });
          }).on('error', (err) => {
            fs.unlink(tarballPath, () => {});
            reject(err);
          });
        });
      } catch (error) {
        console.error('Error downloading and extracting:', error);
      }
    }

    // Start the download and processing pipeline by calling the downloadAndExtract function.
    await downloadAndExtract();

    // Create a SQLite database instance using Knex.
    const db = knex({
      client: 'sqlite3',
      connection: {
        filename: './out/database.sqlite',
      },
      useNullAsDefault: true,
    });

    // Define the schema and create tables for 'organizations' and 'customers' in the SQLite database.
    try {
      await db.schema.createTable('organizations', (table) => {
        table.string('Index').notNullable();
        table.string('OrganizationId').primary();
        table.string('Name').notNullable();
        table.string('Website').notNullable();
        table.string('Country').notNullable();
        table.string('Description').notNullable();
        table.string('Founded').notNullable();
        table.string('Industry').notNullable();
        table.string('NumberOfEmployees').notNullable();
      });
    } catch (error) {
      console.error('Error creating organizations table:', error);
    }

    try {
      await db.schema.createTable('customers', (table) => {
        table.string('Index').notNullable();
        table.string('CustomerId').primary();
        table.string('FirstName').notNullable();
        table.string('LastName').notNullable();
        table.string('Company');
        table.string('City').notNullable();
        table.string('Country').notNullable();
        table.string('Phone1');
        table.string('Phone2');
        table.string('Email').notNullable();
        table.string('SubscriptionDate').notNullable();
        table.string('Website').notNullable();
      });
    } catch (error) {
      console.error('Error creating customers table:', error);
    }

    // Process the 'customers' CSV file.
    processCsvFile('customers', './tmp/dump.tar/dump/customers.csv', {
      columns: ['Index', 'CustomerId', 'FirstName', 'LastName', 'Company', 'City', 'Country', 'Phone1', 'Phone2', 'Email', 'SubscriptionDate', 'Website'],
    });

    // Process the 'organizations' CSV file.
    processCsvFile('organizations', './tmp/dump.tar/dump/organizations.csv', {
      columns: ['Index', 'OrganizationId', 'Name', 'Website', 'Country', 'Description', 'Founded', 'Industry', 'NumberOfEmployees'],
    });
  } catch (error) {
    console.error('Error in processDataDump:', error);
  }
}

// Define a function to process a CSV file and insert data into the database.
function processCsvFile(tableName: string, filePath: string, options: { columns: string[] }) {
  try {
    const { columns } = options;
    const db = knex({
      client: 'sqlite3',
      connection: {
        filename: './out/database.sqlite',
      },
      useNullAsDefault: true,
    });

    // Read the CSV file and parse it.
    const fileStream = fs.createReadStream(filePath);
    const csvParser = parse({
      delimiter: ',',
      columns,
    });

    let buffer: any[] = [];
    let rowCount = 0;
    let flag = 0;

    fileStream.pipe(csvParser);

    csvParser.on('data', (row) => {
      if (flag !== 0) {
        buffer.push(row);
        rowCount++;
      }
      if (flag === 0) {
        flag = 1;
      }

      if (rowCount >= 100) {
        // Batch insert data into the specified table.
        try {
          db.batchInsert(tableName, buffer, buffer.length);
        } catch (error) {
          console.error(`Error batch inserting data into ${tableName}: ${error}`);
        }
        buffer = [];
        rowCount = 0;
      }
    });

    csvParser.on('end', () => {
      if (buffer.length > 0) {
        // Insert remaining rows into the table.
        try {
          db.batchInsert(tableName, buffer, buffer.length);
        } catch (error) {
          console.error(`Error inserting remaining data into ${tableName}: ${error}`);
        }
        buffer = [];
      }
    });

    csvParser.on('error', (error) => {
      console.error(error);
    });
  } catch (error) {
    console.error('Error in processCsvFile:', error);
  }
}