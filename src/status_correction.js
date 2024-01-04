const pkg = require("scramjet")
const fs = require("fs");
const { parse } = require("csv-parse");
const axios = require("axios");

const { DataStream } = pkg;

const config = {
    options: {
        url:`https://ipa-id-int.rdc-qa.moveaws.com/users/correct_status_for_soft_delete`,
        method: "DELETE"
    },
    headers: {
      "Content-Type": "application/json",
      "Authorization": "Bearer <token>",
      "Connection": "keep-alive"
    }
};

let counter = 0;
async function runDatastream(parser) {
    await DataStream
        .from(parser)
        .setOptions({ maxParallel: 30 })
        .do(async (row) => {
            const payload = JSON.parse(row);
            const memberID = payload.member_id;
            console.log(memberID)

            try {
                // Read the member ID & call correct_status_for_soft_delete with it appended

                const response = await axios({
                    url: `${config.options.url}/${memberID}`,
                    method: config.options.method,
                    headers: config.headers
                })
                if (response.status != 200){
                    fs.writeFile("output.csv", `${memberID}\n`, (err) => {
                        if (err){
                            counter ++
                            console.log(counter)
                            }
                        console.log(`Error with member ID: ${memberID}`)
                        })
                    }} catch (error) {
                    console.log(error)
                    fs.writeFile("output.csv", `${error.config.data}\n`, (err) =>{
                        if(err){
                        console.log(err);
                        counter ++
                        console.log(`ERRORED MEMBER IS: ${memberID}`)}
                    })};
        })
    .run();
}

async function sendStream () {
    try {
        fs.readdir("./data", async (err, files) => {
        if (err) {
            console.log(`Exiting with ERROR: `, err);
            process.exit(1);
        }
        for (let i = 0; i < files.length; i++) {
            
                console.log(`Working on: ${files[i]}`)
                const parser = fs
                .createReadStream(`./Running/${files[i]}`)
                .pipe(parse({
                delimiter: ",", from_line: 1
                }));
                await runDatastream(parser)
            console.log(`Finished ${files[i]}`)
                
        }})
    } catch (error){
        console.log(`ERROR THROWN WITH: ${memberID}`)
    };
}

sendStream()