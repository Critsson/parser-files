const data = require("./blue_cross_toc.json");
const { reporting_structure } = data;
const csv = require("fast-csv");
const fs = require("fs");

const csvStream = csv.format({ headers: ["Link"] });
const writeStream = fs.createWriteStream("./links.csv");
const urls = new Set();

for (let reportingObj of reporting_structure) {
  const { in_network_files } = reportingObj;
  for (let file of in_network_files) {
    const { location } = file;
    if (urls.has(location)) {
      continue;
    } else {
      urls.add(location);
      csvStream.write([location]);
    }
  }
}

csvStream.pipe(writeStream);
