var express = require('express');
var router = express.Router();
var kafka = require('./kafka/client');
/* GET users listing. */

router.post('/search_flights', function(req, res, next) {
    try {

        var user_data = {
            "filter"    : req.body.filter,
            "key"       : "search_flights"
        }
        kafka.make_request('flight_topic',user_data, function(err,response_kafka){
            if(err){
                console.trace(err);
                res.status(401).json({error: err});
            }
            else{
                global.flightSearchResponse = {message: "Success", data : response_kafka};
                res.status(200).send({message: "Success", data : response_kafka});
            }
        });

    }
    catch (e){
        console.log(e);
        res.send(e);
    }
});

router.get('/search_flights', function(req, res){
    //download file functionality
    console.log("Backend: "+ global.flightSearchResponse);
    res.status(201).send(global.flightSearchResponse);
});

module.exports = router;
