/*************************************/
/*                                   */
/*    Testing the Comms class        */
/*        Setup producers            */
/*                                   */
/* usage: node producers.js <number> */
/*                                   */
/*************************************/

require("dotenv").config();
const Comms = require("./amqp.js");

const start = new Date();

const length = isNaN(Number(process.argv[2])) ? 1 : Number(process.argv[2]);

const conns = Array.from({length}, (v, i) => new Comms(
	{
		host: process.env.AMQP_HOST,
		scheme: 'amqps',
		transport: 'tls',
		port: Number(process.env.AMQP_PORT),
		username: process.env.AMQP_UNAME,
		password: process.env.AMQP_PWORD
	},
	{},
	"stupid"
));

conns.forEach( (conn, idx) => {
	conn.on("connected", async () => {
		//console.log("CONNECTED", idx);
		setInterval( async() => {
			const date = new Date();
			const elapsed = date.getTime() - start.getTime();
			var rep = null;
			try{
				rep = await conn.send("dispatcher2/task", { task: "store", "item": { elapsed , idx } });
			}
			catch(err){
				rep = err;
			}
			const date2 = new Date();
			const diff = date2.getTime() - date.getTime();
			//logger.push(`${JSON.stringify(rep)} ${diff} ${date.toISOString()}\n`);
			console.log(rep, diff, date);
		}, 1000);
	});

	conn.on("error", (err) => {
		console.log("GOT AN ERROR", err);
		conn.close();
	});

	conn.on("disconnected", (context) => {
		console.log("DISCONNECTED", context);
	});

	conn.on("closed", () => {
		console.log("CLOSED");
	});

	conn.connect();

});