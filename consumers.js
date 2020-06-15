/*************************************/
/*                                   */
/*    Testing the Comms class        */
/*        Setup consumers            */
/*                                   */
/* usage: node consumers.js <number> */
/*                                   */
/*************************************/

require("dotenv").config();
const Comms = require("./amqp.js");

const start = new Date();

const length = isNaN(Number(process.argv[2])) ? 1 : Number(process.argv[2]);

const servers = Array.from({length}, (v, i) => new Comms(
	{
		store: (comm, message) => {
			const { item } = message;

			console.log("STORING", i, item);

			return item;
		}
	},
	"dispatcher2"
));

servers.forEach( server => {
	server.on("connected", async () => {
		console.log("CONNECTED");
	});

	server.connect({
		host: process.env.AMQP_HOST,
		scheme: 'amqps',
		transport: 'tls',
		port: Number(process.env.AMQP_PORT),
		username: process.env.AMQP_UNAME,
		password: process.env.AMQP_PWORD
	});
});