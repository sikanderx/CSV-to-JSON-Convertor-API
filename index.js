const express = require('express');
const csv = require('csv-parser');
const { Sequelize, DataTypes } = require('sequelize');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3050;

// DATABASE CONNECTION
const sequelize = new Sequelize(process.env.DB_NAME, process.env.DB_USER, process.env.DB_PASSWORD, {
	host: process.env.DB_HOST,
	dialect: process.env.DB_DIALECT,
	port: process.env.DB_PORT,
	logging: false
});

const usersTable = sequelize.define('users', {
	id: {
		type: DataTypes.INTEGER,
		primaryKey: true,
		autoIncrement: true
	},
	name: {
		type: DataTypes.JSONB,
		allowNull: false
	},
	age: {
		type: DataTypes.INTEGER,
		allowNull: false
	},
	address: DataTypes.JSONB,
	additional_info: DataTypes.JSONB
});

sequelize.sync()
	.then(() => console.log('Database & tables synced'))
	.catch(err => console.error('Error syncing database', err));

app.use(express.json());


// API
app.post('/convert', (req, res) => {
	if (!req.body.csvData) {
		return res.status(400).json({ error: 'CSV data is required.' });
	}

	const csvData = req.body.csvData;
	const jsonArray = [];

	csvData.pipe(csv()).on('data', (row) => {
		const new_row = {
			name: row['name.firstName'] + ' ' + row['name.lastName'],
			age: row.age,
			address: JSON.parse(row.address),
			additional_info: JSON.parse(row.additional_info)
		};
		jsonArray.push(new_row);
	}).on('end', () => {
		usersTable.bulkCreate(jsonArray).then(() => {
			const ad = calculateAgeDistribution();

			console.log('Age Distribution Report:');
			console.log('-------------------------');
			console.log(`Age Group < 20: ${ad.lt20}%`);
			console.log(`Age Group 20-40: ${ad.between20And40}%`);
			console.log(`Age Group 40-60: ${ad.between40And60}%`);
			console.log(`Age Group > 60: ${ad.gt60}%`);
			console.log('-------------------------');

			res.json({ message: 'CSV data saved successfully.' });
		}).catch((err) => {
			res.status(500).json({ error: err.message });
		});
	}).on('error', (err) => {
		res.status(500).json({ error: err.message });
	});
});

app.listen(PORT, () => {
	console.log(`Server is running on port ${PORT}`);
});


// CALCULATE AGE DISTRIBUTION
async function calculateAgeDistribution() {
	const users = await usersTable.findAll();
	const totalUsers = users.length;

	let lt20 = 0;
	let between20And40 = 0;
	let between40And60 = 0;
	let gt60 = 0;

	users.forEach(user => {
		const age = user.age;
		if (age < 20) {
			lt20++
		} else if (age >= 20 && age <= 40) {
			between20And40++;
		} else if (age > 40 && age <= 60) {
			between40And60++;
		} else gt60++;
	});

	let result = {
		lt20: ((lt20 / totalUsers) * 100).toFixed(2),
		between20And40: ((between20And40 / totalUsers) * 100).toFixed(2),
		between40And60: ((between40And60 / totalUsers) * 100).toFixed(2),
		gt60: ((gt60 / totalUsers) * 100).toFixed(2)
	};

	return result;
}