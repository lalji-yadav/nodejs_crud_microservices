const userService = require('../services/userService');

async function createUser(req, res) {
    const userData = req.body;

    try {
        await userService.publishToQueue(userService.QUEUE_NAMES.CREATE_USER, userData);
        res.status(202).send('User creation in progress'); // Respond immediately
    } catch (error) {
        console.error('Error publishing to queue:', error);
        res.status(500).send('Internal Server Error');
    }
}

async function getUser(req, res) {
    const userId = req.params.id;

    try {
        await userService.publishToQueue(userService.QUEUE_NAMES.GET_USER, { id: userId });
        res.status(202).send('User retrieval in progress'); // Respond immediately
    } catch (error) {
        console.error('Error publishing to queue:', error);
        res.status(500).send('Internal Server Error');
    }
}

async function updateUser(req, res) {
    const userData = { id: req.params.id, ...req.body };

    try {
        await userService.publishToQueue(userService.QUEUE_NAMES.UPDATE_USER, userData);
        res.status(202).send('User update in progress'); // Respond immediately
    } catch (error) {
        console.error('Error publishing to queue:', error);
        res.status(500).send('Internal Server Error');
    }
}

async function deleteUser(req, res) {
    const userId = req.params.id;

    try {
        await userService.publishToQueue(userService.QUEUE_NAMES.DELETE_USER, { id: userId });
        res.status(202).send('User deletion in progress'); // Respond immediately
    } catch (error) {
        console.error('Error publishing to queue:', error);
        res.status(500).send('Internal Server Error');
    }
}

module.exports = {
    createUser,
    getUser,
    updateUser,
    deleteUser
};
