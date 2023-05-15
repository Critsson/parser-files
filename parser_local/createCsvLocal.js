const fs = require("fs");
const JSONStream = require("JSONStream");
const csv = require("fast-csv");

const createCsv = (path) => {
  const readStream = fs.createReadStream(path);
  const codeSet = new Set([
    "90791",
    "90834",
    "90837",
    "90846",
    "90847",
    "90849",
    "90853",
    "92507",
    "92508",
    "92521",
    "92522",
    "92523",
    "92524",
    "92526",
    "92605",
    "92607",
    "92608",
    "92609",
    "92610",
    "96111",
    "96112",
    "96113",
    "96130",
    "96131",
    "96136",
    "96137",
    "97150",
    "97151",
    "97152",
    "97153",
    "97155",
    "97156",
    "97157",
    "97165",
    "97166",
    "97167",
    "97168",
    "97530",
    "97535",
    "0373T",
    "G9012",
    "H0031",
    "H0032",
    "H2012",
    "H2019",
    "H2020",
  ]);
  const jsonParseStream = JSONStream.parse("*.*");
  const jsonStream = readStream.pipe(jsonParseStream);
  const csvStream = csv.format({
    headers: [
      "npi",
      "name",
      "negotiation_arrangement",
      "billing_code_type",
      "billing_code_type_version",
      "billing_code",
      "description",
      "negotiated_type",
      "negotiated_rate",
      "expiration_date",
      "billing_class",
    ],
  });

  jsonParseStream.on("data", (data) => {
    if (
      data.hasOwnProperty("negotiation_arrangement") &&
      codeSet.has(data.billing_code)
    ) {
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
        if (negotiated_rates[i].hasOwnProperty("provider_groups")) {
          const { negotiated_prices, provider_groups } = negotiated_rates[i];
          for (let provider of provider_groups) {
            const npi = provider.npi[0];
            const {
              negotiated_type,
              negotiated_rate,
              expiration_date,
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
            const bufferNotFull = csvStream.write(row);
            console.log(`Writing ${name} into csv`);
            if (!bufferNotFull) {
              jsonParseStream.pause();
            }
          }
        }
      }
    } else {
      console.log("Searching for codes...");
    }
  });

  csvStream.on("drain", () => {
    jsonParseStream.resume();
  });

  jsonStream.on("end", () => {
    jsonParseStream.end();
  });
  const writeStream = fs.createWriteStream("./benchmark.csv", { flags: "a" });
  csvStream.pipe(writeStream);
};

createCsv(
  "./files/2023-04-01_Blue-Cross-and-Blue-Shield-of-Massachusetts-Inc_Blue-Care-Elect-Fully-Insured_in-network-rates.json"
);

module.exports = createCsv;
