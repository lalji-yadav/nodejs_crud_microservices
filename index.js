const express = require('express');
const mongoose = require('mongoose');
const userController = require('./controllers/userController');
require('./consumers/userConsumer'); // Include the consumer here

const app = express();
app.use(express.json());

// Connect to MongoDB
mongoose.connect('mongodb://localhost/ecommerce', { useNewUrlParser: true, useUnifiedTopology: true })
    .then(() => console.log('Connected to MongoDB'))
    .catch(err => console.error('MongoDB connection error:', err));

// User CRUD Routes
app.post('/users', userController.createUser);
app.get('/users/:id', userController.getUser);
app.put('/users/:id', userController.updateUser);
app.delete('/users/:id', userController.deleteUser);

// Start the server
app.listen(3000, () => {
    console.log('User Service running on port 3000');
});
