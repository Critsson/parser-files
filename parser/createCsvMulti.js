const axios = require("axios");
const zlib = require("zlib");
const fs = require("fs");
const JSONStream = require("JSONStream");
const csv = require("fast-csv");
const { pipeline } = require("node:stream/promises");

const createCsvMulti = async (url, urls, topLevel, secondLevel) => {
  if (urls.has(url)) {
    return "Url already parsed";
  } else {
    urls.add(url);
  }

  try {
    const response = await axios({
      url,
      method: "GET",
      responseType: "stream",
    });

    const writeStream = fs.createWriteStream("./benchmark.csv", {
      flags: "a",
    });
    const gunzipStream = zlib.createGunzip();
    const jsonParseStream = JSONStream.parse("*.*");
    const csvStream = csv.format({ headers: true });
    response.data.pipe(gunzipStream).pipe(jsonParseStream);

    jsonParseStream.on("data", (data) => {
      if (data.hasOwnProperty("negotiation_arrangement")) {
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
              console.log(
                `Top Iteration: ${topLevel}, Lower Iteration: ${secondLevel} writing ${name} into csv`
              );
              if (!bufferNotFull) {
                jsonParseStream.pause();
              }
            }
          } else {
            console.log("Searching");
          }
        }
      }
    });

    gunzipStream.on("end", () => {
      gunzipStream.end();
    });

    csvStream.on("drain", () => {
      jsonParseStream.resume();
    });

    jsonParseStream.on("end", () => {
      jsonParseStream.end();
    });

    await pipeline(csvStream, writeStream);
    csvStream.end();
    writeStream.end();
  } catch (err) {
    console.error(err);
    return new Promise((resolve, reject) => {
      reject("Failed");
    });
  }
  return "Success";
};

module.exports = createCsvMulti;
