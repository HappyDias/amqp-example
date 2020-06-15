const EventEmitter = require('events');
const amqp = require('rhea');
const {nanoid} = require('nanoid')

/**
 * CORE COMMUNICATION CLASS
 * Description: Global class to be used to ensure communications through AMQP
 * 
 */
class Comms extends EventEmitter {
    /**
     * @param  {Object} connDetails connection details for [rhea]{@link https://github.com/amqp/rhea#connectoptions}, 
     * @param  {Object} tasks Object containing tasks, each task is an asynchronous function with two arguments: 1. the instance of the communication class; 2. the body of the message of the task to preform
     * @param  {str} listener String defining the listening (or listening prefix) for queues
     * @param  {Object} extra Optional object to customize instance, 'id': A prefix for all listening queues for multiple instances using the same tasks
     */
    constructor(tasks, listener, extra = {}){
        const { id } = extra;

        super(); 
        this.tasks = tasks; // Local storing tasks
        this.listener = id ? `${listener}/${id}` : listener; // Customizing queue prefix
        this.operations = {}; // Object containing the handlers for promises created when tasks are requested
        this.id = id; // locally store id
        this.connection = null;
        this.container = amqp.create_container(); //Maybe add an id?

        // Initializes stuff
        this.Initialization()
    }

    /**
     * @param {Object} connDetails The object provided in the constructor 
     */
    Initialization(){

        // Check for open connection to setup consumers
        this.container.on('connection_open', context => {
            const taskRcvr = context.connection.open_receiver(`${this.listener}/task`); // Listener for task requests

            // Listening for task requests - must ensure the task is run, the 
            taskRcvr.on("message", async context => {
                const { message } = context;
                const { body, reply_to, correlation_id } = message;
                const { task } = body;
                const response = { // Generic response object
                    to: reply_to, // Not sure if it is necessary
                    correlation_id // Necessary to ensure the reply gets processed properly
                }

                if(task){ // If you send a message to this listener, it must include a task
                    if(task in this.tasks){ // Check if task exists in tasks object
                        try{
                            response.body = { // Proper body assuming everything works right
                                code: 1,
                                result: await this.tasks[task](this, body)
                            };
                        }
                        catch(error){ // Catching an error encountered during the task
                            response.body = {code: -3, message: `Task runtime error ${error}`}
                        }
                    }
                    else{ // Sending back an error in case the task is not supported
                        response.body = {code: -2, message: `Unsupported task '${task}' requested`}
                    }
                }
                else{ // Sending back an error if a task name is not supplied
                    response.body = {code: -1, message: `No task requested`};
                }

                const sender = context.connection.open_sender(reply_to); // Creating a sender to send the result back to the requester
                const msgCtx = context;

                // Recommended way of sending a message, the sender must acquire
                sender.once("sendable", context => {
                    sender.send(response); //Actually sending the message
                    msgCtx.delivery.accept(); //Accept the retrieved message
                    sender.close();
                });

            });

            this.emit("connected");
        });

        // Listening for a connection error - emiting its own error event, must improve in the future
        this.container.on('connection_error', (err) => {
            this.emit('error', err);
        });

        // Listening for a protocol error - emiting its own error event, must improve in the future
        this.container.on("protocol_error", (err) => {
            this.emit('error', err);

        });

        this.container.on("disconnected", (context) => {
            this.emit('disconnected', context);
        });

        // Listening for a generic error - emiting its own error event, must improve in the future
        this.container.on("error", (err) => {
            console.log("ERROR", err)
            this.emit('error', err);
        });
    }

    /**
     * @param  {Object, str} target Queue/topic to send a message to
     * @param  {Object} message An object containing the message to be send - if a task is required to be preformed that key must be sent
     * @param  {Object} extra customize sending defaults
     * @return {Promise} Awaitable promise to recieve the result of the request
     */
    send(target, message, extra = {}){
        return new Promise((resolve, reject) => { // Do I return the promise? I would want to await it
            const correlation_id = nanoid();
            const sender = this.connection.open_sender(target);
            const {timeout, broadcast} = extra;

            sender.once("sendable", (context) => {
                // Listen to the result for this task
                //const resultRcvr = this.connection.open_receiver(reply_to);
                const resultRcvr = this.connection.open_receiver({
                    source: {dynamic: true}
                });

                // Listening for task results - must ensure no errors were recieved and resolve/reject the underlying task promise
                resultRcvr.on("message", context => {
                    const { message } = context;
                    const { body, correlation_id } = message;
                    const { code } = body;

                    // Check if the operation is underway on this instance
                    if(this.operations[correlation_id]){
                        if(code > 0){ // Result codes larger than 0 is a success
                            this.operations[correlation_id].resolve(body);
                        }
                        else{ // Result code smaller than or equal to 0 represent a failure
                            this.operations[correlation_id].reject(body);
                        }
                        clearTimeout(this.operations[correlation_id].timeout); // Clears timeout function
                        delete this.operations[correlation_id]; // Regardless of result operation is finished
                    }
                    resultRcvr.close();
                });

                // Do I want to check this message object?
                resultRcvr.on("receiver_open", (context) => {

                    const reply_to = context.receiver.source.address;
                    //resultRcvr.credit()

                    const sent = sender.send({
                        reply_to,
                        body: message,
                        correlation_id,
                        ttl: timeout ? timeout : 5000
                    });

                    if(broadcast){
                        resolve(sent);
                    }
                    else{
                        // Setting a timeout for the sending of a function in case it does not get properly handled
                        const t_o = setTimeout(
                            () => {
                                if(this.operations[correlation_id]){
                                    this.operations[correlation_id].reject({code: -4, message: `Request timed out`});
                                    delete this.operations[correlation_id]
                                }
                                resultRcvr.close();
                                sender.close();
                            },
                            timeout ? timeout : 10000
                        );
                        // When sending a request must set a unique operation id with the functions to resolve or reject the underlying promise
                        this.operations[correlation_id] = {resolve, reject, timeout: t_o}; 
                    }
                    sender.close();
                });
            });
        });

    }
    /**
     * Starts the connection setup in the initialization
     * @return {rhea.Connection} Instance of the established connection
     */
    connect(connDetails){
        // Instantiating the connection object 
        if(!this.connection){
            this.connDetails = connDetails;
            this.connection = this.container.connect(this.connDetails);
        }
        return this.connection;
    }

    /**
     * Closing function to stop listening to requests
     * @return {bool}
     */
    close(){
        if(this.connection){
            this.connection.close();
        }
        this.emit("closed");
    }

}

module.exports = Comms;