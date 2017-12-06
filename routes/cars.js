var express = require('express');
var router = express.Router();
var kafka = require('./kafka/client');
/* GET users listing. */

router.post('/search_cars', function(req, res, next) {
    try {

        var user_data = {
            "filter"    : req.body.filter,
            "key"       : "search_cars"
        }
        kafka.make_request('car_topic',user_data, function(err,response_kafka){
            if(err){
                console.trace(err);
                res.status(401).json({error: err});
            }
            else{
                global.carSearchResponse = {message: "Success", data : response_kafka};
                console.log("CAR RESPONSE:"+JSON.stringify(global.carSearchResponse))
                res.status(200).send({message: "Success", data : response_kafka});
            }
        });

    }
    catch (e){
        console.log(e);
        res.send(e);
    }
});

router.get('/search_cars', function(req, res){
    //download file functionality
    console.log("Backend: "+ global.carSearchResponse);
    res.status(201).send(global.carSearchResponse);
});

module.exports = router;
