const express = require('express');
const amqp = require('amqplib/callback_api');
const bodyParser = require('body-parser');
const mongoose = require('mongoose');

// Set up the Express app
const app = express();
app.use(bodyParser.json());

// MongoDB model for User
const User = mongoose.model('User', new mongoose.Schema({
    username: { type: String, required: true },
    email: { type: String, required: true }
}));

// RabbitMQ connection details
const RABBITMQ_URL = 'amqp://localhost';
const QUEUE_NAMES = {
    CREATE_USER: 'user_creation',
    GET_USER: 'user_retrieval',
    UPDATE_USER: 'user_update',
    DELETE_USER: 'user_deletion'
};

// Function to connect to RabbitMQ
function connectToRabbitMQ() {
    amqp.connect(RABBITMQ_URL, (error, connection) => {
        if (error) throw error;
        connection.createChannel((error, channel) => {
            if (error) throw error;

            // Ensure the queues exist
            Object.values(QUEUE_NAMES).forEach(queue => {
                channel.assertQueue(queue, { durable: true });
            });

            // Start consuming messages for each queue
            channel.consume(QUEUE_NAMES.CREATE_USER, (msg) => {
                const userData = JSON.parse(msg.content.toString());
                saveUser(userData);
                channel.ack(msg);
            });

            channel.consume(QUEUE_NAMES.GET_USER, (msg) => {
                const userId = JSON.parse(msg.content.toString()).id;
                getUser(userId).then(user => {
                    console.log('Retrieved User:', user);
                });
                channel.ack(msg);
            });

            channel.consume(QUEUE_NAMES.UPDATE_USER, (msg) => {
                const userData = JSON.parse(msg.content.toString());
                updateUser(userData);
                channel.ack(msg);
            });

            channel.consume(QUEUE_NAMES.DELETE_USER, (msg) => {
                const userId = JSON.parse(msg.content.toString()).id;
                deleteUser(userId);
                channel.ack(msg);
            });
        });
    });
}

// Function to save user to the database
function saveUser(userData) {
    const newUser = new User(userData);
    newUser.save()
        .then(() => console.log('User created:', userData))
        .catch(err => console.error('Error saving user:', err));
}

// Function to retrieve user from the database
function getUser(userId) {
    return User.findById(userId)
        .then(user => {
            if (user) {
                console.log('User retrieved:', user);
                return user;
            } else {
                console.log('User not found:', userId);
                return null;
            }
        })
        .catch(err => console.error('Error retrieving user:', err));
}

// Function to update user in the database
function updateUser(userData) {
    User.findByIdAndUpdate(userData.id, userData, { new: true })
        .then(updatedUser => console.log('User updated:', updatedUser))
        .catch(err => console.error('Error updating user:', err));
}

// Function to delete user from the database
function deleteUser(userId) {
    User.findByIdAndDelete(userId)
        .then(() => console.log('User deleted:', userId))
        .catch(err => console.error('Error deleting user:', err));
}

// User creation endpoint
app.post('/users', (req, res) => {
    const userData = req.body;

    // Publish to RabbitMQ
    amqp.connect(RABBITMQ_URL, (error, connection) => {
        if (error) throw error;
        connection.createChannel((error, channel) => {
            if (error) throw error;

            channel.assertQueue(QUEUE_NAMES.CREATE_USER, { durable: true });
            channel.sendToQueue(QUEUE_NAMES.CREATE_USER, Buffer.from(JSON.stringify(userData)), { persistent: true });

            console.log('User creation request sent:', userData);
            res.status(202).send('User creation in progress'); // Respond immediately
        });
    });
});

// User retrieval endpoint
app.get('/users/:id', (req, res) => {
    const userId = req.params.id;

    // Publish to RabbitMQ
    amqp.connect(RABBITMQ_URL, (error, connection) => {
        if (error) throw error;
        connection.createChannel((error, channel) => {
            if (error) throw error;

            channel.assertQueue(QUEUE_NAMES.GET_USER, { durable: true });
            channel.sendToQueue(QUEUE_NAMES.GET_USER, Buffer.from(JSON.stringify({ id: userId })), { persistent: true });

            console.log('User retrieval request sent:', userId);
            res.status(202).send('User retrieval in progress'); // Respond immediately
        });
    });
});

// User update endpoint
app.put('/users/:id', (req, res) => {
    const userData = { id: req.params.id, ...req.body };

    // Publish to RabbitMQ
    amqp.connect(RABBITMQ_URL, (error, connection) => {
        if (error) throw error;
        connection.createChannel((error, channel) => {
            if (error) throw error;

            channel.assertQueue(QUEUE_NAMES.UPDATE_USER, { durable: true });
            channel.sendToQueue(QUEUE_NAMES.UPDATE_USER, Buffer.from(JSON.stringify(userData)), { persistent: true });

            console.log('User update request sent:', userData);
            res.status(202).send('User update in progress'); // Respond immediately
        });
    });
});

// User delete endpoint
app.delete('/users/:id', (req, res) => {
    const userId = req.params.id;

    // Publish to RabbitMQ
    amqp.connect(RABBITMQ_URL, (error, connection) => {
        if (error) throw error;
        connection.createChannel((error, channel) => {
            if (error) throw error;

            channel.assertQueue(QUEUE_NAMES.DELETE_USER, { durable: true });
            channel.sendToQueue(QUEUE_NAMES.DELETE_USER, Buffer.from(JSON.stringify({ id: userId })), { persistent: true });

            console.log('User delete request sent:', userId);
            res.status(202).send('User deletion in progress'); // Respond immediately
        });
    });
});

// Start the server and connect to RabbitMQ
app.listen(3000, () => {
    console.log('User Service running on port 3000');
    connectToRabbitMQ(); // Start the RabbitMQ consumer
});

// Connect to MongoDB
mongoose.connect('mongodb://localhost/ecommerce', { useNewUrlParser: true, useUnifiedTopology: true })
    .then(() => console.log('Connected to MongoDB'))
    .catch(err => console.error('MongoDB connection error:', err));

