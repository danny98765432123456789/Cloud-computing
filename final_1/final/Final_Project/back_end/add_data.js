require('coffee-script/register')
var _ = require('underscore');
var mongoose = require('mongoose');
var request = require('request');

var parkinglot = require('./models/parkinglot');


mongoose.connect('mongodb://localhost/cloud-final');

var newparkinglot = new parkinglot({
                park_name: "新莊思賢立體停車場",
                park_id: 50051
            });
 newparkinglot.save()
