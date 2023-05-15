import fs from "fs";

const getJsons = async () => {
  const { reporting_structure } = data;

  for (let reportingObj of reporting_structure) {
    if (reportingObj.in_network_files.length > 0) {
      for (let inNetworkFile of reportingObj.in_network_files) {
        fs.appendFileSync(
          "blue_cross_links.txt",
          `${inNetworkFile.location}\n`
        );
      }
    }
  }
};

export default getJsons;
