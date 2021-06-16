const csv = require('csv-parser');
const fs = require('fs');
const async = require('async');
const http = require('http');
const intervalTime = process.env.INTERVAL || 5 * 60 * 1000; // defualt 5 mintes

const options = {
  hostname: '127.0.0.1',
  port: 3000,
  path: '/hook',
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
  }
};


const notify = (message) => {
  const req = http.request(options, (res) => {
    console.log(message);
  });

  req.on('error', function(e) {
    console.log('Problem with request: ' + e.message);
  });
  //send the whole traffic data to the hook
  req.write(JSON.stringify(message));
  req.end();
}


let result = [];
fs.createReadStream('./data/trafficData158324.csv')
  .pipe(csv())
  .on('data', (row) => {
    result.push(row);
  })
  .on('end', () => {
    console.log('CSV file successfully processed, total records:' + result.length);
    console.log('Start to send notify data with time interval:' + intervalTime);
    async.eachSeries(result, function(data, next) {
      setTimeout(function() {
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
    }, function() {

      console.log('All done!');
    });
  });