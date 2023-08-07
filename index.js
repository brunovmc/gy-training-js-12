const express = require('express');
const fs = require('fs');
const csvParser = require('csv-parser');

const app = express();
const port = 3000;

async function calculateHourlyAggregation(file) {
  const hourlyData = {};
  let currentHourStart = null;

  const promise = () => new Promise((resolve, reject) => {
    fs.createReadStream(file)
      .pipe(csvParser({ separator: ';' }))
      .on('data', (row) => {
        const date = parseDateTime(row.dateTime.replace(',','.'));
  
        if (currentHourStart === null) {
          currentHourStart = new Date(date.getFullYear(), date.getMonth(), date.getDate(), date.getHours(), 0, 0);
        }
  
        const nextHourStart = new Date(currentHourStart.getTime() + 60 * 60 * 1000);
        
        if (date >= nextHourStart) {
          currentHourStart = new Date(date.getFullYear(), date.getMonth(), date.getDate(), date.getHours(), 0, 0);
        }
  
        const hourStartString = formatDateTime(currentHourStart);
        const value = Number(row.value.replace(',', '.'));
  
        if (!isNaN(value)) {
          if (!hourlyData[hourStartString]) {
            hourlyData[hourStartString] = { sum: 0, count: 0};
          }
  
          hourlyData[hourStartString].sum += value;
          hourlyData[hourStartString].count++;
  
        }
      })
      .on('end', () => {
        console.log("Hourly Aggregation Report:");
        for (const hourStartDate in hourlyData) {
          const { sum, count } = hourlyData[hourStartDate];
          const hourlyAverage = sum / count;
          console.log(`date ${hourStartDate} - Average Energy Usage: ${hourlyAverage.toFixed(2)} kWh`);
        }
        resolve();
      });
    });
    return await promise();
}

function parseDateTime(dateTimeString) {
  const [datePart, timePart] = dateTimeString.split(' ');

  const [day, month, year] = datePart.split('/');
  const [hour, minute] = timePart.split(':');
  
  return new Date(year, month - 1, day, hour, minute);
}

function formatDateTime(date) {
  const day = String(date.getDate()).padStart(2, '0');
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const year = date.getFullYear();
  const hour = String(date.getHours()).padStart(2, '0');

  return `${day}-${month}-${year} ${hour}:00:00`;
}

app.get('/generate-report', async (req, res) => {
  const file = 'METRICS_REPORT-1673351714089 (2).csv';
  await calculateHourlyAggregation(file);
  res.send("Hourly Aggregation Report generated and logged in the console.");
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
