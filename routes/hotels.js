var express = require('express');
var router = express.Router();
var kafka = require('./kafka/client');
/* GET users listing. */

router.post('/search_hotels', function(req, res, next) {
    try {

        var user_data = {
            "filter"  : req.body.filter,
            "key"       : "search_hotels"
        }
        kafka.make_request('hotel_topic',user_data, function(err,response_kafka){
            if(err){
                console.trace(err);
                res.status(401).json({error: err});
            }
            else{
                res.status(200).send({message: "Success", data : response_kafka});
            }

        });

    }
    catch (e){
        console.log(e);
        res.send(e);
    }
});

router.post('/add_hotel', function(req, res, next) {
    try {

        var user_data = {
            "hotel_name"  : req.body.hotel_name,
            "hotel_address"  : req.body.hotel_address,
            "zip_code"  : req.body.zip_code,
            "hotel_stars"  : req.body.hotel_stars,
            "hotel_ratings"  : req.body.hotel_ratings,
            "description"  : req.body.description,
            "city"  : req.body.city,
            "hotel_image"  : req.body.hotel_image,
            "key"       : "add_hotel"
        }
        kafka.make_request('hotel_topic',user_data, function(err,response_kafka){
            console.log("IN RESPONSE");
            if(err){
                console.trace(err);
                res.status(401).json({error: err});
            }
            else{
                console.log("IN SUCCESS BACKEND");
                res.status(200).send({message: "Success", data : response_kafka});
            }

        });

    }
    catch (e){
        console.log(e);
        res.send(e);
    }
});

router.post('/update_hotel', function(req, res, next) {
    try {

        var user_data = {
            "hotel_name"  : req.body.hotel_name,
            "hotel_address"  : req.body.hotel_address,
            "zip_code"  : req.body.zip_code,
            "hotel_stars"  : req.body.hotel_stars,
            "hotel_ratings"  : req.body.hotel_ratings,
            "description"  : req.body.description,
            "city"  : req.body.city,
            "hotel_image"  : req.body.hotel_image,
            "key"       : "update_hotel"
        }
        kafka.make_request('hotel_topic',user_data, function(err,response_kafka){
            console.log("IN RESPONSE");
            if(err){
                console.trace(err);
                res.status(401).json({error: err});
            }
            else{
                console.log("IN SUCCESS BACKEND");
                res.status(200).send({message: "Success", data : response_kafka});
            }

        });

    }
    catch (e){
        console.log(e);
        res.send(e);
    }
});


module.exports = router;
