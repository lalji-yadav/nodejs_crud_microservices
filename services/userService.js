const User = require('../models/userModel');
const amqp = require('amqplib/callback_api');
const RABBITMQ_URL = 'amqp://localhost';

const QUEUE_NAMES = {
    CREATE_USER: 'user_creation',
    GET_USER: 'user_retrieval',
    UPDATE_USER: 'user_update',
    DELETE_USER: 'user_deletion'
};

// Function to save user to the database
function saveUser(userData) {
    const newUser = new User(userData);
    return newUser.save();
}

// Function to retrieve user from the database
async function getUser(userId) {
    return await User.findById(userId);
}

// Function to update user in the database
function updateUser(userData) {
    return User.findByIdAndUpdate(userData.id, userData, { new: true });
}

// Function to delete user from the database
function deleteUser(userId) {
    return User.findByIdAndDelete(userId);
}

// Function to publish messages to RabbitMQ
function publishToQueue(queue, data) {
    return new Promise((resolve, reject) => {
        amqp.connect(RABBITMQ_URL, (error, connection) => {
            if (error) return reject(error);
            connection.createChannel((error, channel) => {
                if (error) return reject(error);

                channel.assertQueue(queue, { durable: true });
                channel.sendToQueue(queue, Buffer.from(JSON.stringify(data)), { persistent: true });
                console.log(`${queue} request sent:`, data);
                resolve();
            });
        });
    });
}

module.exports = {
    saveUser,
    getUser,
    updateUser,
    deleteUser,
    publishToQueue,
    QUEUE_NAMES
};
