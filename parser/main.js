const data = require("./blue_cross_toc.json");
const createCsvMulti = require("./createCsvMulti.js");
const { reporting_structure } = data;

const main = async () => {
  const urls = new Set();
  for (let i = 0; i < reporting_structure.length; i++) {
    const { in_network_files } = reporting_structure[i];
    if (in_network_files.length > 0) {
      for (let j = 14; j < 15; j++) {
        try {
          await createCsvMulti(in_network_files[j].location, urls, i, j);
          console.log("finished");
        } catch (err) {
          console.error(err);
        }
      }
    }
  }
};

main();
