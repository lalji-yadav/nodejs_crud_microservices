const User = require('../models/userModel');
const amqp = require('amqplib/callback_api');
const RABBITMQ_URL = 'amqp://localhost';
const QUEUE_NAMES = {
    CREATE_USER: 'user_creation',
    GET_USER: 'user_retrieval',
    UPDATE_USER: 'user_update',
    DELETE_USER: 'user_deletion'
};

// Function to connect to RabbitMQ and consume messages
function connectToRabbitMQ() {
    amqp.connect(RABBITMQ_URL, (error, connection) => {
        if (error) throw error;
        connection.createChannel((error, channel) => {
            if (error) throw error;

            // Ensure the queues exist
            Object.values(QUEUE_NAMES).forEach(queue => {
                channel.assertQueue(queue, { durable: true });
            });

            // Start consuming messages
            channel.consume(QUEUE_NAMES.CREATE_USER, async (msg) => {
                const userData = JSON.parse(msg.content.toString());
                try {
                    await saveUser(userData);
                    channel.ack(msg);
                } catch (err) {
                    console.error('Error saving user:', err);
                    channel.nack(msg);
                }
            });

            channel.consume(QUEUE_NAMES.GET_USER, async (msg) => {
                const userId = JSON.parse(msg.content.toString()).id;
                // Implement retrieval logic here if needed
                console.log('Retrieve user:', userId);
                channel.ack(msg);
            });

            channel.consume(QUEUE_NAMES.UPDATE_USER, async (msg) => {
                const userData = JSON.parse(msg.content.toString());
                await updateUser(userData);
                channel.ack(msg);
            });

            channel.consume(QUEUE_NAMES.DELETE_USER, async (msg) => {
                const userId = JSON.parse(msg.content.toString()).id;
                await deleteUser(userId);
                channel.ack(msg);
            });
        });
    });
}

// Function to save user to the database
function saveUser(userData) {
    const newUser = new User(userData);
    return newUser.save();
}

// Function to update user in the database
function updateUser(userData) {
    return User.findByIdAndUpdate(userData.id, userData, { new: true });
}

// Function to delete user from the database
function deleteUser(userId) {
    return User.findByIdAndDelete(userId);
}

// Start consuming messages
connectToRabbitMQ();
