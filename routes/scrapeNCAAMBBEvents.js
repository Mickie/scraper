var nodeio = require('node.io');
var pg = require('pg');
var theDBUrl = process.env.DATABASE_URL || "tcp://fanzo_site:fanzo_site@localhost/fanzo_site_development";

exports.scrapeEvents = function (req, res) {
    console.log("The run started!");
    var theDB = new pg.Client(theDBUrl);
    theDB.connect();

    var start_date = new Date(2013, 1, 27);  //2012-11-09
    var end_date = new Date(2013, 2, 10);  //2012-03-10
    var totaldays = Math.floor((end_date.getTime() - start_date.getTime()) / (1000 * 60 * 60 * 24)) + 1;

    var formatDate = function (value) {

        if (value.getMonth() <= 8 && value.getDate() <= 9) {

            return value.getFullYear() + "0" + (value.getMonth() + 1) + "0" + value.getDate();
        }

        else if (value.getMonth() <= 8) {

            return value.getFullYear() + "0" + (value.getMonth() + 1) + value.getDate();
        }

        else if (value.getDate() <= 9) {

            return value.getFullYear().toString() + (value.getMonth() + 1) + "0" + value.getDate();
        }

        else {
            return value.getFullYear().toString() + (value.getMonth() + 1) + value.getDate();
        }
    };

    var aaa = new Array();
    for (var i = 0; i < totaldays; i++) {
        var myDate=start_date;
        aaa.push(formatDate(myDate));
        myDate.setDate(myDate.getDate()+1);
    }

    var theScrapeJob;
    theScrapeJob = new nodeio.Job

        ({


            input:aaa,


            run:function (anIndex) {
                var theJob = this;
                var theUrl = "http://espn.go.com/mens-college-basketball/schedule";


                theUrl += "?date=" + anIndex;


                console.log("preparing to scrape URL:" + theUrl);

                this.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.163 Safari/535.19');
                this.getHtml(theUrl, function (err, $) {
                    var theEvents = new Array();
                    var theEventsForToday = new Object();
                    theEventsForToday.date = anIndex;
                    theEventsForToday.events = new Array();


                    console.log("Scraping events for:" + theEventsForToday.date);


                    var theHandler = function (anIndex, aTRElement) {
                        var theEvent = new Object();
                        var theClasses = $(aTRElement).attr("class").split(" ");
                        theEvent.visitingTeamId = theClasses[1].replace("team-41-", "");
                        theEvent.homeTeamId = theClasses[2].replace("team-41-", "");


                        theEvent.time = $(aTRElement).children().first().text();
                        theEvent.visitingTeamName = $(aTRElement).find("td:first-child > a:first-child").text();
                        theEvent.homeTeamName = $(aTRElement).find("td:first-child > a:last-child ").text();
                        console.log("Found team name: " + theEvent.homeTeamName + " for id: " + theEvent.homeTeamId);
                        theEventsForToday.events.push(theEvent);
                    };
                    $("table.tablehead").find("tr.oddrow").each(theHandler);
                    $("table.tablehead").find("tr.evenrow").each(theHandler);

                    theEvents.push(theEventsForToday);


                    theJob.emit(theEvents);
                });
            },
            reduce:function (aListOfEvents) {
                var theFanzoIdFinder = new FanzoIdFinder(aListOfEvents, this, theDB);
                theFanzoIdFinder.mapEspnIdsToFanzoIds();
            },
            output:function (aListOfEvents) {
                var theFanzoEventCreator = new FanzoEventCreator(aListOfEvents, this, theDB);
                theFanzoEventCreator.addEvents();
            },
            complete:function (aCallback) {
                theDB.end();
                res.send("completed scraping events");
                aCallback();
            }

        });


    nodeio.start(theScrapeJob,
        {
            max:1,
            take:1,
            retries:2,
            wait:2,
            auto_retry:false,
            timeout:false,
            global_timeout:false,
            flatten:true,
            benchmark:true,
            jsdom:true,
            external_resources:false,
            redirects:3
        },
        function (anError) {
            if (anError) {
                console.log(anError);
                //theDB.end();
                res.send(anError);
            }
        },
        false);

}

var FanzoEventCreator = function (aListOfEvents, aJob, aDB) {
    this.myListOfEvents = aListOfEvents;
    this.myJob = aJob;
    this.myDB = aDB;

    this.myTotalNumberOfEvents = 0;
    this.myEventsCompleted = 0;

    this.addEvents = function () {
        for (var i = 0, j = this.myListOfEvents.length; i < j; i++) {
            var theEventsForADay = this.myListOfEvents[i];
            this.myTotalNumberOfEvents += theEventsForADay.events.length;
            for (var k = 0, l = theEventsForADay.events.length; k < l; k++) {
                var theEvent = theEventsForADay.events[k];

                console.log("creating event for:" + theEventsForADay.date);

                this.myDB.query(
                    {
                        name:'add event',
                        text:"insert into events(name, home_team_id, visiting_team_id, event_date, event_time, created_at, updated_at) values('game', $1, $2, $3, $4, now(), now()) returning id",
                        values:[theEvent.homeTeamFanzoId, theEvent.visitingTeamFanzoId, theEventsForADay.date, this.getTime(theEvent.time)]
                    },
                    function (anError, aNewEventIdResult) {
                        if (anError) {
                            console.log("ERROR adding event: " + anError);
                        }
                        console.log(aNewEventIdResult);
                        console.log("event added at id: " + aNewEventIdResult.rows[0].id);
                    });

            }
        }
    };

    this.getTime = function (aTimeString) {
        if (aTimeString == "TBA" || aTimeString == "Postponed") {
            return null;
        }
        else {
            console.log("Incoming date is: " + aTimeString);
            var theTimeParts = aTimeString.split(" ");
            console.log("theTimeParts: " + theTimeParts);
            if (theTimeParts[1].toUpperCase() == "PM") {
                theHoursSeconds = theTimeParts[0].split(":");
                if (theHoursSeconds[0]=="12"){theHoursSeconds[0]="12";}
                else{
                theHoursSeconds[0] = (parseInt(theHoursSeconds[0]) + 12).toString();
                }
                theTimeParts[0] = theHoursSeconds.join(":");
            }

            return theTimeParts[0];
        }
    }
}

var FanzoIdFinder = function (aListOfEvents, aJob, aDB) {
    this.myListOfEvents = aListOfEvents;
    this.myJob = aJob;
    this.myDB = aDB;

    this.myTotalNumberOfEvents = 0;
    this.myEventsCompleted = 0;

    this.mapEspnIdsToFanzoIds = function () {
        for (var i = 0, j = this.myListOfEvents.length; i < j; i++) {
            var theEventsForADay = this.myListOfEvents[i];
            this.myTotalNumberOfEvents += theEventsForADay.events.length;
            for (var k = 0, l = theEventsForADay.events.length; k < l; k++) {
                var theEvent = theEventsForADay.events[k];
                var theTeamQueryHandler = new TeamQueryHandler(theEvent, this.myDB, this.getCompleteCallback());
                theTeamQueryHandler.findTeamIds();
            }
            ;
        }
        ;
    };

    this.onEventComplete = function () {
        this.myEventsCompleted++;
        if (this.myEventsCompleted >= this.myTotalNumberOfEvents) {
            this.myJob.emit(this.myListOfEvents);
        }
    };

    this.getCompleteCallback = function () {
        var theCallback = this.onEventComplete;
        var theThis = this;
        return function () {
            return theCallback.call(theThis)
        };
    }

}

var TeamQueryHandler = function (anEvent, aDB, aCompleteCallback) {
    this.myEvent = anEvent;
    this.myDB = aDB;
    this.myCompleteCallback = aCompleteCallback;



    this.findTeamIds = function () {
        var theDB = this.myDB;
        var theCompleteCallback = this.myCompleteCallback;
        var theEvent = this.myEvent;

        theDB.query(
            {
                name:'find team id',
                text:'select id from teams where espn_team_id=$1',
                values:[theEvent.visitingTeamId]
            },
            function (anError, aVisitingTeamIdResult) {
                this.getTodayDate = function () {
                    var today = new Date();
                    return today.getFullYear() + "-" + (today.getMonth() + 1 ) + "-" + today.getDate();
                }

                if (aVisitingTeamIdResult && aVisitingTeamIdResult.rows.length > 0) {

                    console.log("visiting team found at id: " + aVisitingTeamIdResult.rows[0].id);
                    theEvent.visitingTeamFanzoId = aVisitingTeamIdResult.rows[0].id;

                }

                else {
                    console.log("visiting team " + theEvent.visitingTeamId + " (" + theEvent.visitingTeamName + ") not found, adding");
                    theDB.query(
                        {
                            name:'add visiting team',
                            text:'insert into teams(name,espn_team_id,sport_id,league_id,location_id,created_at, updated_at) values($1,$2,$3,$4,$5,$6,$7) returning id',
                            values:[theEvent.visitingTeamName, theEvent.visitingTeamId, 1, 2, 4, this.getTodayDate(), this.getTodayDate()]
                        },
                        function (anError, anInsertVisitingTeamIdResult) {
                            if (anError) {
                                console.log("ERROR adding visitingTeam: " + anError);
                            }

                            console.log("add visiting team " + theEvent.visitingTeamId + " created at id: " + anInsertVisitingTeamIdResult.rows[0].id);
                            theEvent.visitingTeamFanzoId = anInsertVisitingTeamIdResult.rows[0].id;


                        });


                }


                theDB.query(
                    {
                        name:'find team id',
                        text:'select id from teams where espn_team_id=$1',
                        values:[theEvent.homeTeamId]
                    },
                    function (anError, aHomeTeamIdResult) {
                        this.getTodayDate = function () {
                            var today = new Date();
                            return today.getFullYear() + "-" + (today.getMonth() + 1 ) + "-" + today.getDate();
                        }


                        if (aHomeTeamIdResult && aHomeTeamIdResult.rows.length > 0) {
                            console.log("home team " + theEvent.homeTeamId + " (" + theEvent.homeTeamName + ") not found, adding");
                            theEvent.homeTeamFanzoId = aHomeTeamIdResult.rows[0].id;
                            theCompleteCallback();
                        }

                        else {
                            console.log("home team " + theEvent.homeTeamId + " not found, adding");
                            theDB.query(
                                {
                                    name:'add home team',
                                    text:'insert into teams(name,espn_team_id,sport_id,league_id,location_id,created_at, updated_at) values($1,$2,$3,$4,$5,$6,$7) returning id',
                                    values:[theEvent.homeTeamName, theEvent.homeTeamId, 1, 2, 4, this.getTodayDate(), this.getTodayDate()]
                                },
                                function (anError, anInsertHomeTeamIdResult) {
                                    if (anError) {
                                        console.log("ERROR adding homeTeam: " + anError);
                                    }

                                    console.log("add visiting team " + theEvent.homeTeamId + " created at id: " + anInsertHomeTeamIdResult.rows[0].id);
                                    theEvent.homeTeamFanzoId = anInsertHomeTeamIdResult.rows[0].id;

                                    theCompleteCallback();
                                });
                        }


                    });
            });
    };


}
