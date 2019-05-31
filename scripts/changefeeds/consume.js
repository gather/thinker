const fs = require("fs");
const r = require("rethinkdbdash")({
    db: "gather"
});
const { getConn } = require("./queue_manager");

const QUEUE = "changes.files_changes";

async function consumer(msg) {
    const conn = await getConn();
    const data = JSON.parse(msg.content);
    
    if (data.type === "add" && data.new_val) {
        try {
            logToDisk(data.new_val.id)
            await r.table("files").insert(data.new_val).run();
            conn.ack(msg);
        } catch(e) {
            conn.nack(msg, undefined, false)  // nack({ requeue: false })
        }
    } else if (data.type === "change" && data.new_val) {
        try {
            logToDisk(data.new_val.id)
            await r.table("files").get(data.new_val.id).replace(data.new_val).run();
            conn.ack(msg);
        } catch(e) {
            conn.nack(msg, undefined, false)  // nack({ requeue: false })
        }
    } else if (data.type === "remove" && !data.new_val) {
        try {
            logToDisk(data.old_val.id)
            await r.table("files").get(data.old_val.id).delete().run();
            conn.ack(msg);
        } catch(e) {
            conn.nack(msg, undefined, false)  // nack({ requeue: false })
        }
    }
}

function logToDisk(id) {
    try {
        fs.appendFileSync("./consumer.log", id + "\n")
    } catch(e) {
        console.log(`Error write ID to disk - ${id}`)
    }
}

(async () => {
    try {
        const conn = await getConn();
        await conn.consume(QUEUE, consumer)
    } catch (e) {
        console.log("Consumer fail, logging...");
        console.error(e)
        // fs.appendFile("./error.log", JSON.stringify(msg) + "\n", (err) => {
        //     if (err) console.log(err);
        //     console.error(e)
        // });
    }
})();
