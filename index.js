const fs = require('fs');
const { parse } = require("csv-parse");
const data = [];
const parser = parse({ columns: true }, () => { });

fs.createReadStream(__dirname + '/query_data.csv').pipe(parser).on('data', (row) => {
    data.push(
        {
            ...row,
            siteid: row.siteid.split("&")[0],
            username: row.username.split(" ")[0],
        })
}).on('end', () => {
    const createCsvWriter = require('csv-writer').createObjectCsvWriter;
    const csvWriter = createCsvWriter({
        path: 'output.csv',
        header: [
            { id: 'siteid', title: 'siteid' },
            { id: 'username', title: 'username' },
            { id: 'count', title: 'count' },
        ]
    });
    csvWriter
        .writeRecords(findOcc(data, "username"))
        .then(() => console.log('The CSV file was written successfully'));
});


function findOcc(arr, key) {
    let arr2 = [];

    arr.forEach((x) => {

        // Checking if there is any object in arr2
        // which contains the key value
        if (arr2.some((val) => {
            return val[key] == x[key]
        })) {

            // If yes! then increase the occurrence by 1
            arr2.forEach((k) => {
                if (k[key] === x[key]) {
                    k["count"]++
                }
            })

        } else {
            // If not! Then create a new object initialize 
            // it with the present iteration key's value and 
            // set the occurrence to 1
            let a = {}
            a[key] = x[key]
            a["count"] = 1
            a["siteid"] = x.siteid
            arr2.push(a);
        }
    })
    return arr2
}

