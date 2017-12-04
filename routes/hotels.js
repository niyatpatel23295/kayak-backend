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
                 global.hotelSearchResponse = {message: "Success", data : response_kafka};
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
            "deluxNo": req.body.deluxNo,
            "deluxDescription":req.body.deluxDescription,
            "premiumNo": req.body.premiumNo,
            "premiumDescription":req.body.premiumDescription,
            "standardNo": req.body.standardNo,
            "standardDescription":req.body.standardDescription,
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

/*router.post('/update_hotel', function(req, res, next) {
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
});*/



router.post('/get_hotel_details', function(req,res){
    try{
        var hotel_key = req.body.searchHotel_key;
        console.log("hotel_key:"+hotel_key);

        var Search_SQL = "SELECT * FROM hotels where hotel_name='"+hotel_key+"'";
        if(hotel_key!= ''){
            mysql.executequery(Search_SQL, function (err, result) {
                if (err) {
                    console.log(err);
                }
                else {
                    console.log("result of hotel sql "+result);
                    res.send({"data":result});
                }
            })
        }else{
            res.send({"error_message":"Hotel Name doesn't exists"})
        }

    }catch(e){
        console.log(e);
    }
})



router.post('/update_hotel', function(req,res){
    try{

        var hotel_name = req.body.hotel_name;
        var hotel_address = req.body.hotel_address;
        var zip_code = req.body.zip_code;
        var hotel_stars = req.body.hotel_stars;
        var hotel_ratings = req.body.hotel_ratings;
        var description = req.body.description;
        var city = req.body.city;

        console.log("hotel_name:"+hotel_name);

        var update_SQL = "UPDATE hotels SET hotel_name = '" + hotel_name +
            "',hotel_address='"+hotel_address+
            "',zip_code='"+zip_code+
            "',hotel_stars='"+hotel_stars+
            "',hotel_ratings='"+hotel_ratings+
            "',description='"+description+
            "' where hotel_name ='"+hotel_name+"';";

        console.log("query is "+update_SQL);

            mysql.executequery(update_SQL, function (err, result) {
                if (err) {
                    console.log(err);
                }
                else {
                    console.log("result of hotel update query "+result);
                    res.send({"data":"update successful"});
                }
            })

    }catch(e){
        console.log(e);
    }
})


//get selected hotel to book
router.post('/get_selected_hotel', function (req, res, next) {
    try {
        var hotel_data = req.body.filter;
        console.log(hotel_data);
        global.selectedHotelDetails = {message: "Success", data: hotel_data};
        res.status(200).send({message: "Success", data: hotel_data});
    }
    catch (e) {
        console.log(e);
        res.send(e);
    }
});

router.get('/get_selected_hotel', function (req, res) {
    //download file functionality
    console.log("Backend: " + global.selectedHotelDetails);
    res.status(201).send(global.selectedHotelDetails);
    //return global.hotelSearchResponse;
});

router.get('/search_hotels', function(req, res){
   //download file functionality
   console.log("Backend: "+ global.hotelSearchResponse);
   res.status(201).send(global.hotelSearchResponse);
   //return global.hotelSearchResponse;
});

module.exports = router;
