const csv = require("csv-parser");
const fs = require("fs");
const async = require("async");
const http = require("http");
const intervalTime = process.env.INTERVAL || 100; // defualt 100 ms

const options = {
  hostname: "127.0.0.1",
  port: 3000,
  path: "/hook",
  method: "POST",
  headers: {
    "Content-Type": "application/json",
  },
};

const notify = (message) => {
  const req = http.request(options, (res) => {
    console.log(message);
  });
  req.on("error", function (e) {
    console.log("Problem with request: " + e.message);
  });
  //send the whole traffic data to the hook
  req.write(JSON.stringify(message));
  req.end();
};
let totalRecords = 0;
const dataDic = "./data/traffic_june_sep/";
fs.readdir(dataDic, (err, files) => {
  async.eachSeries(
    files,
    function (file, cb) {
      let result = [];
      fs.createReadStream(dataDic + file)
        .pipe(csv())
        .on("data", (row) => {
          result.push(row);
        })
        .on("end", () => {
          console.log(
            "Read CSV file:" +
              file +
              " successfully processed, total records:" +
              result.length
          );
          totalRecords += result.length;
          console.log(
            "Start to send notify data with time interval:" + intervalTime
          );
          async.eachSeries(
            result,
            function (data, next) {
              setTimeout(function () {
                /*
              data Example:
                {
                status: 'OK',
                avgMeasuredTime: '53',
                avgSpeed: '69',
                extID: '668',
                medianMeasuredTime: '53',
                TIMESTAMP: '2014-06-09T05:25:00',
                vehicleCount: '0',
                _id: '14353465',
                REPORT_ID: '158324'
              }
              */
                notify(data);
                next(); // don't forget to execute the callback!
              }, intervalTime);
            },
            function () {
              cb();
              console.log(
                "CSV file:" + file + "is done! Total:" + totalRecords
              );
            }
          );
        });
    },
    function () {
      console.log("All done!");
    }
  );
});
