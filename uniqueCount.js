const fs = require('fs');
const { parse } = require("csv-parse");

const data = [];
const parser = parse({ columns: true }, () => { });

fs.createReadStream(__dirname + '/query_data.csv').pipe(parser).on('data', (row) => {
    data.push({
        siteid: row.siteid.split("&")[0],
        username: row.username.split(" ")[0],
    })
}).on('end', () => {
    const createCsvWriter = require('csv-writer').createObjectCsvWriter;
    const csvWriter = createCsvWriter({
        path: 'uniqueCount.csv',
        header: [
            { id: 'siteid', title: 'siteid' },
            { id: 'count', title: 'count' },
        ]
    });
    csvWriter
        .writeRecords(getUserCount(data))
        .then(() => console.log('The CSV file was written successfully'));
});


function getUserCount(data) {
    let dataMap = {};
    const output = [];

    data.forEach((element) => {
        dataMap[element.siteid] = dataMap[element.siteid] + 1 || 1;
    });

    for (let char in dataMap) {
        let arr = [];
        data.forEach((element) => {
            if (element.siteid === char && !arr.includes(element.username)) {
                arr.push(element.username);
                dataMap[char] = arr;
            }
        });
    }

    for (let char in dataMap) {
        let obj = {};
        obj.siteid = char;
        obj.count = dataMap[char].length;
        output.push(obj);
    }
    return output;
}

