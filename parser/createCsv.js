const axios = require("axios");
const zlib = require("zlib");
const fs = require("fs");
const JSONStream = require("JSONStream");
const csv = require("fast-csv");

const createCsv = async (url) => {
  try {
    const response = await axios({
      url,
      method: "GET",
      responseType: "stream",
    });
    const gunzipStream = zlib.createGunzip();
    const jsonParseStream = JSONStream.parse("in_network.*");
    const jsonStream = response.data.pipe(gunzipStream).pipe(jsonParseStream);
    const csvStream = csv.format({ headers: true });

    gunzipStream.on("end", () => {
      gunzipStream.end();
    });
    jsonStream.on("data", (data) => {
      const {
        negotiation_arrangement,
        name,
        billing_code_type,
        billing_code_type_version,
        billing_code,
        description,
        negotiated_rates,
      } = data;
      for (let i = 0; i < negotiated_rates.length; i++) {
        const { negotiated_prices, provider_groups } = negotiated_rates[i];
        for (let provider of provider_groups) {
          const npi = provider.npi[0];
          const {
            negotiated_type,
            negotiated_rate,
            expiration_date,
            service_code,
            billing_class,
          } = negotiated_prices[0];
          const row = [
            npi,
            name,
            negotiation_arrangement,
            billing_code_type,
            billing_code_type_version,
            billing_code,
            description,
            negotiated_type,
            negotiated_rate,
            expiration_date,
            billing_class,
          ];
          csvStream.write(row);
          console.log(`Writing ${name} into csv`);
        }
      }
    });
    jsonStream.on("end", () => {
      jsonParseStream.end();
    });
    const writeStream = fs.createWriteStream("./benchmark.csv", { flags: "a" });
    csvStream.pipe(writeStream);

    return new Promise((resolve, reject) => {
      writeStream.on("finish", () => {
        csvStream.end();
        writeStream.end();
        resolve("File finished parsing");
      });
      writeStream.on("error", (err) => {
        reject(err);
      });
    });
  } catch (error) {
    console.error(error);
  }
};

createCsv(
  "https://transparency-in-coverage.bluecrossma.com/2023-04-01_Blue-Cross-and-Blue-Shield-of-Massachusetts-Inc_Blue-Care-Elect-Fully-Insured_in-network-rates.json.gz"
);

module.exports = createCsv;
