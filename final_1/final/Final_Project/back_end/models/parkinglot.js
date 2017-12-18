var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var parkinglot = new Schema({
  park_payex: String,
  park_address: String,
  park_latitude: Number,
  park_summary  : Number,
  park_servicetime: String,
  park_area: String,
  park_totalcar: Number,
  park_longitude: Number,
  park_name: String,
  park_id   : Number
});
module.exports = mongoose.model('parkinglot', parkinglot);
