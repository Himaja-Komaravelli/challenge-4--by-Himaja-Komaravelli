/**
 * The entry point function. This will read the provided CSV file, scrape the companies'
 * YC pages, and output structured data in a JSON file.
 * 
 */
import fs from 'fs';
import csv from 'csv-parser';
import { CheerioCrawler } from 'crawlee';

// Define the structure of the scraped data
interface CompanyData {
  profile: YCProfile;
}

// Define the structure for YC profile data
interface YCProfile {
  name: string | null;
  long_description: string | null;
  year_founded: number | null;
  team_size: number | null;
  location: string | null;
  country: string | null;
  urls: URLS | null;
}

// Define the structure for URLs data
interface URLS {
  linkedin_url: string | null;
  twitter_url: string | null;
  fb_url: string | null;
  cb_url: string | null;
  github_url: string | null;
}

// Define the structure for company information
interface Company {
  name: string;
  url: string;
}

// Read the list of companies from a CSV file
async function readCompanyList(): Promise<Company[]> {
  const companies: Company[] = [];

  return new Promise((resolve) => {
    fs.createReadStream('inputs/companies.csv')
      .pipe(csv())
      .on('data', (row: any) => {
        const company: Company = {
          name: row["Company Name"],
          url: row["YC URL"],
        };
        companies.push(company);
      })
      .on('end', () => {
        resolve(companies);
      });
  });
}

// Scrape YC profile data for a given company
async function scrapeYCProfile(company: Company): Promise<YCProfile> {
  const ycProfile: YCProfile = {
    name: null,
    long_description: null,
    year_founded: null,
    team_size: null,
    location: null,
    country: null,
    urls: null,
  };

  const crawler = new CheerioCrawler({
    async requestHandler({ request, $, enqueueLinks, log }) {
      const title = $('title').text();
      const classs = $('script.js-react-on-rails-component').text();
      const jsonData = JSON.parse(classs);
      const longDescription = jsonData.company.long_description || null;
      const founded = jsonData.company.year_founded || null;
      const team_size = jsonData.company.team_size || null;
      const location = jsonData.company.location || null;
      const country = jsonData.company.country || null;
      const linkedin_url = jsonData.company.linkedin_url || null;
      const twitter_url = jsonData.company.twitter_url || null;
      const fb_url = jsonData.company.fb_url || null;
      const cb_url = jsonData.company.cb_url || null;
      const github_url = jsonData.company.github_url || null;

      ycProfile.name = title;
      ycProfile.long_description = longDescription;
      ycProfile.year_founded = founded;
      ycProfile.team_size = team_size;
      ycProfile.location = location;
      ycProfile.country = country;
      ycProfile.urls = {
        linkedin_url,
        twitter_url,
        fb_url,
        cb_url,
        github_url,
      };

      // Extract links from the current page and add them to the crawling queue.
      await enqueueLinks();
    },

    // Limit the number of requests to ensure safe crawling
    maxRequestsPerCrawl: 200,
  });

  // Initialize an array to store the company URL
  const urlsToCrawl = [];
  urlsToCrawl.push(company.url);

  // Start the crawl with the company's URL
  await crawler.run(urlsToCrawl);

  return ycProfile;
}

// Write the scraped data to a JSON file
async function writeScrapedDataToFile(data: CompanyData[]) {
  const outputFileName = 'out/scraped.json';
  const dirname='out'
  if (!fs.existsSync(dirname)) {
    fs.mkdirSync(dirname, { recursive: true});
  }  fs.writeFileSync(outputFileName, JSON.stringify(data, null, 2));
  console.log("✅ Done!");
}

// Main function to process the list of companies
export async function processCompanyList() {
  const companies = await readCompanyList();
  const scrapedData: CompanyData[] = [];

  for (const company of companies) {
    const profileData = await scrapeYCProfile(company);
    scrapedData.push({ profile: profileData });
  }

  await writeScrapedDataToFile(scrapedData);
}

