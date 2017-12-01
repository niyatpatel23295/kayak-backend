var passport = require("passport");
var LocalStrategy = require("passport-local").Strategy;
//var mongo = require("./mongo");
//var mongoURL = "mongodb://localhost:27017/demo3";
var kafka = require('./kafka/client');

module.exports = function(passport) {
    passport.use('login', new LocalStrategy(function(username , password, done) {
          console.log("Inside Passport login");
        kafka.make_request('user_topic',{"email":username,"password":password, "key": "login_api"}, function(err,results){
            if(err){
                done(err,{});
            }
            else
            {
                console.log("Result in backend:passport js", JSON.stringify(results))
                if(results){
                    done(null,results);
                }
                else {
                    done(null,false);
                }
            }
        });
/*    passport.use('login', new LocalStrategy(function(username   , password, done) {
        try {
            mongo.connect(mongoURL, function(){
                console.log('Connected to mongo at: ' + mongoURL);
                var coll = mongo.collection('creds');

                coll.findOne({username: username, password:password}, function(err, user){
                    if (user) {
                        user.password = undefined;
                        console.log(user);

                        done(null, user);

                    } else {
                        done(null, false);
                    }
                });
            });
        }
        catch (e){
            done(e,{});
        }*/
    }));
};
