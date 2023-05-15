const data = require("./blue_cross_toc.json");
const { reporting_structure } = data;
const fs = require("fs");
const axios = require("axios");
const zlib = require("zlib");
const { pipeline } = require("stream/promises");

const main = async () => {
  const urls = new Set();
  for (let i = 4; i < 5; i++) {
    const { in_network_files } = reporting_structure[i];
    fs.mkdir(`./files/${i + 1}`, (err) => {
      if (err !== null) {
        console.error(err);
      }
    });
    if (in_network_files.length > 0) {
      for (let j = 0; j < in_network_files.length; j++) {
        if (in_network_files[j]) {
          const { location } = in_network_files[j];
          if (urls.has(location)) {
            console.log("Skipped repeated url...");
            continue;
          } else {
            urls.add(location);
          }
          const response = await axios({
            method: "get",
            url: location,
            responseType: "stream",
          });
          const writeStream = fs.createWriteStream(
            `./files/${i + 1}/${i + 1}_${j + 1}.json`
          );
          const gunzipStream = zlib.createGunzip();
          try {
            console.log(`Writing file ${i + 1}_${j + 1}.json...`);
            await pipeline(response.data, gunzipStream, writeStream);
            gunzipStream.end();
            writeStream.end();
          } catch (error) {
            console.error(error);
          }
        } else {
          console.log("Object has undefined location...");
        }
      }
    }
  }
};

main();
