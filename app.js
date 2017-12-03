var express = require('express');

var path = require('path');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
//var passport = require('passport');
var cors = require('cors');
var kafka = require('./routes/kafka/client');
var mysql = require("./routes/mysql");

var multer = require('multer');
var storage = multer.memoryStorage();
var upload = multer({ storage: storage });
var fs = require('fs-extra');
var fs_native = require('fs');

var app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

//require('./routes/passport')(passport);



var routes = require('./routes/index');
var hotels = require('./routes/hotels');
var flights = require('./routes/flights');
var cars = require('./routes/cars');
var mongoSessionURL = "mongodb://ec2-54-153-9-233.us-west-1.compute.amazonaws.com:27017/sessions";
var expressSessions = require("express-session");
var mongoStore = require("connect-mongo/es5")(expressSessions);


var mongo = require("mongodb").MongoClient;



// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'ejs');

// uncomment after placing your favicon in /public
//app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')));
app.use(logger('dev'));

var corsOptions = {
    origin: 'http://localhost:3000',
    credentials: true,
}
app.use(cors(corsOptions))

app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));
app.use(expressSessions({
    secret: "CMPE273_passport",
    resave: false,
    //Forces the session to be saved back to the session store, even if the session was never modified during the request
    saveUninitialized: false, //force to save uninitialized session to db.
    //A session is uninitialized when it is new but not modified.
    duration: 30 * 60 * 1000,
    activeDuration: 5 * 6 * 1000,
    store: new mongoStore({
        url: mongoSessionURL
    })
}));
//app.use(passport.initialize());

app.use('/', routes);
app.use('/hotels', hotels);
app.use('/flights', flights);
app.use('/cars', cars);

app.post('/logout', function(req,res) {
    console.log(req.session.user);
    req.session.destroy();
    console.log('Session Destroyed');
    res.status(200).send();
});

app.post('/login', function(req, res) {
    console.log("Inside App.js");
    try {
        var user_data = {

            "email"  : req.body.email,
            "password"  : req.body.password,
            "key"       : "login_api",

        }
        console.log("UserData ",user_data);
        kafka.make_request('user_topic',user_data, function(err,response_kafka){
            if(err){
                console.trace(err);
                res.status(401).json({error: err});
            }
            else{
                console.log("login response ", JSON.stringify(response_kafka));
                req.session.user = req.body.email;
                req.session.save();
                console.log(req.session.user);
                res.status(201).send({message: "Success", data : response_kafka});

            }

        });

    }
    catch (e){
        console.log(e);
        res.send(e);
    }



});

app.post('/signup', function(req, res) {
    try {
        console.log(user_data);
        var user_data = {
            "firstName"  : req.body.firstName,
            "lastName"  : req.body.lastName,
            "address"     : req.body.address,
            "city" : req.body.city,
            "zipCode"  : req.body.zipCode,
            "phoneNo"  : req.body.phoneNo,
            "email"  : req.body.email,
            "password"  : req.body.password,
            "profilephoto"  : req.body.profilephoto,
            "key"       : "signup_api",

        }
        kafka.make_request('user_topic',user_data, function(err,response_kafka){
            if(err){
                console.trace(err);
                res.status(401).json({error: err});
            }
            else{
                console.log("Signup user response ", JSON.stringify(response_kafka));
                res.status(200).send({message: "Success", data : response_kafka});
            }

        });

    }
    catch (e){
        console.log(e);        
        res.send(e);
    }
});


app.post('/addFlight', function(req,res){
    try{
        console.log("Inside Add Flight");
        var flightName = req.body.flight_name;
        var toAirport = req.body.to_airport;
        var fromAirport = req.body.from_airport;
        var departure = req.body.departure;
        var arrival = req.body.arrival;
        var fClass = req.body.class;
        var fair = req.body.fair;
        var flightNumber = req.body.flight_number;
        var duration = req.body.duration;
        console.log("flight Name "+flightName)
        console.log("flight Name "+toAirport)
        console.log("flight Name "+fromAirport)
        //var Search_SQL = "SELECT * FROM flights where hotel_name= "+hotelName;
        var Search_SQL = "insert into flights (flight_name,to_airport,from_airport,departure,arrival,class,fair,flight_number,duration) values('" + flightName + "','" + toAirport + "','" + fromAirport + "','" +departure+ "','" +arrival+ "','" + fClass+ "','" + fair + "','" + flightNumber + "','"+duration+"');";
        console.log("Search_SQL ",Search_SQL);
        mysql.executequery(Search_SQL, function (err, result) {
            if (err) {
                console.log(err);
            }
            else {
                console.log("result of hotel details sql "+result);
                res.json({"data":result});
            }
        })
    }catch(e){
        console.log(e);
    }
});



app.post('/listdir', function(req, res) {
    try {
        console.log("/listdir", req.body.username);
        kafka.make_request('new_topic_2',{"username":req.body.username, "path":req.body.path, "key": "list_directory_api"}, function(err,response_kafka){
            if(err){
                console.trace(err);
                res.status(401).json({error: err});
            }
            else{
                console.log("listdir user response ", JSON.stringify(response_kafka));
                res.status(200).send({message: "Success", data : response_kafka});
            }

        });

    }
    catch (e){
    }
});

app.post('/uploadfile', upload.single('file'), function(req, res) {
    try {
        console.log("/upladfile", req.body.username);
        kafka.make_request('new_topic_2',{"username":req.body.username, "path": req.body.path,"originalname": req.file.originalname,"encoding": req.file.encoding, "buffer": req.file.buffer ,"key": "upload_dir_api"}, function(err,response_kafka){
            if(err){
                console.trace(err);
                res.status(401).json({error: err});
            }
            else{
                console.log("listdir user response ", JSON.stringify(response_kafka));
                res.status(200).send({message: "Success", data : response_kafka});
            }

        });

    }
    catch (e){

    console.log(e)
    }
});

app.post('/downloadfile',  function(req, res) {
    try {
        console.log("/downloadfile", req.body.username);
        kafka.make_request('new_topic_2',{"username":req.body.username, "key": "download_file_api", "path": req.body.path}, function(err,response_kafka){
            if(err){
                console.trace(err);
                res.status(401).json({error: err});
            }
            else{
                console.log("hit here")
                var options = {
                    root: '/' ,
                    dotfiles: 'deny',
                    headers: {
                        'x-timestamp': Date.now(),
                        'x-sent': true
                    }
                }
                var file_buf = new Buffer(response_kafka)
                fs.writeFileSync('/tmp' + req.body.path, file_buf);

                // console.log("listdir user response ", JSON.stringify(response_kafka));
                res.sendFile('/tmp' + req.body.path ,options,function (err, res) {
                    if(err){
                        console.log(err);
                    }
                    else{
                        console.log(res);
                    }
                });
            }

        });
    }
    catch (e){
        console.log(e)
    }
});

app.get('/getCities', function(req,res){
   try{
       var Search_SQL = "SELECT city_name FROM city ";

       mysql.executequery(Search_SQL, function (err, result) {
           if (err) {
               console.log(err);
           }
           else {
               console.log("result of city sql "+result);
               res.json({"data":result});
           }
       })
   }catch(e){
       console.log(e);
   }
})

app.post('/addCars', function(req,res){
    try{
        var car_model = req.body.car_model;
        var no_passangers = req.body.no_passangers;
        var no_largebags = req.body.no_largebags;
        var no_door = req.body.no_door;
        var car_class = req.body.car_class;
        var price = req.body.price;
        var c_name = req.body.city;
        var pickup_address = req.body.pickup_address;
        var cid ='';

        var findCid = "select cid from city where city_name = '"+c_name+"';";
        mysql.executequery(findCid, function (err, result) {
            if (err) {
                console.log(err);
            }
            else {
                cid = result[0].cid;
                console.log("result of City sql "+cid);
                var Search_SQL = "INSERT INTO cars (car_model,cid,no_passangers,no_largebags,no_door,car_class,price,pickup_address) " +
                    "VALUES('"+car_model+"','"+cid+"',"+no_passangers+","+no_largebags+","+no_door+",'"+car_class+"',"+price+",'"+pickup_address+"')";

                console.log(Search_SQL);
                mysql.executequery(Search_SQL, function (err, result) {
                    if (err) {
                        console.log(err);
                    }
                    else {
                        console.log("result of CAR sql "+result);
                        res.json({"data":result});
                    }
                })
            }
        })
    }catch(e){
        console.log(e);
    }
})

app.post('/sharefile',  function(req, res) {
    try {
        console.log("/sharefile");
        kafka.make_request('new_topic_2',{"username":req.body.username, "shareWith": req.body.sharewith, "path": req.body.path, "key": "share_file_api"}, function(err,response_kafka){
            if(err){
                console.trace(err);
                res.status(401).json({error: err});
            }
            else{
                res.status(200).json({"message": "success", "data": response_kafka});
            }

        });
    }
    catch (e){
        console.log(e)
    }
});

app.post('/updateFlight', function(req,res){
    try{
        console.log("Inside Update Flight");
        var fid = req.body.searchFlight_key;
        var flightName = req.body.flight_name;
        var toAirport = req.body.to_airport;
        var fromAirport = req.body.from_airport;
        var departure = req.body.departure;
        var arrival = req.body.arrival;
        var fClass = req.body.class;
        var fair = req.body.fair;
        var flightNumber = req.body.flight_number;
        var duration = req.body.duration;
        console.log("flight Name "+flightName);
        console.log("flight Name "+toAirport);
        console.log("flight Name "+fromAirport);

        // UPDATE Customers
        // SET ContactName = 'Alfred Schmidt', City= 'Frankfurt'
        // WHERE CustomerID = 1;

        //var Search_SQL = "SELECT * FROM flights where hotel_name= "+hotelName;
        var Search_SQL = "UPDATE flights SET flight_name = '" + flightName +
            "',to_airport='"+toAirport+
            "',from_airport='"+fromAirport+
            "',departure='"+departure+
            "',arrival='"+arrival+
            "',class='"+fClass+
            "',fair='"+fair+
            "',flight_number='"+flightNumber+
            "',duration="+duration+
            " where fid ="+fid;
        mysql.executequery(Search_SQL, function (err, result) {
            if (err) {
                console.log(err);
            }
            else {
                console.log("result of hotel details sql "+result);
                res.json({"data":result});
            }
        })
    }catch(e){
        console.log(e);
    }
});

app.post('/GetFlightDetails', function(req,res){
    try{
        var flight_key = req.body.searchFlight_key;
        console.log("flight_key:"+flight_key);

        var Search_SQL = "SELECT * FROM flights where fid='"+flight_key+"'";
        if(flight_key!= ''){
            mysql.executequery(Search_SQL, function (err, result) {
                if (err) {
                    console.log(err);
                }
                else {
                    console.log("result of flight sql "+result[0]);
                    res.send({"data":result});
                }
            })
        }else{
            res.send({"error_message":"Flight name doesn't exists!"})
        }

    }catch(e){
        console.log(e);
    }
});

module.exports = app;
