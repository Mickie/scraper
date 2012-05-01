var nodeio = require('node.io');
var pg = require('pg');
var theDBUrl = process.env.DATABASE_URL || "tcp://fanzo_site:fanzo_site@localhost/fanzo_site_development";


exports.scrapeEvents = function(req, res)
{
  var theDB = new pg.Client(theDBUrl);
  theDB.connect();

  var theScrapeJob = new nodeio.Job
  ({
//    input: [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15],
    input: [1],
    run: function(anIndex) 
    {
      var theJob = this;
      var theUrl = "http://espn.go.com/college-football/schedule";
      
      if (anIndex > 1)
      {
        theUrl += "/_/week/" + anIndex;
      }
      
      console.log("preparing to scrape URL:" + theUrl);
      
      this.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.163 Safari/535.19');
      this.getHtml(theUrl, function(err, $)
      {
        var theEventsForThisWeek = new Array();
        $("table.tablehead").each(function(anIndex, anElement) 
        {
          var theEventsForToday = new Object();
          theEventsForToday.date = $(anElement).find("tr.stathead td").text();
         
          console.log("Scraping events for:" + theEventsForToday.date); 
          
          theEventsForToday.events = new Array();
          var theHandler = function(anIndex, aTRElement)
          {
            var theEvent = new Object();
            var theClasses = $(aTRElement).attr("class").split(" ");
            theEvent.visitingTeamId = theClasses[1].replace("team-23-","");
            theEvent.homeTeamId = theClasses[2].replace("team-23-", "");
            theEvent.time = $(aTRElement).children().first().text();
            theEventsForToday.events.push(theEvent);
          };
          $(anElement).find("tr.oddrow").each(theHandler);
          $(anElement).find("tr.evenrow").each(theHandler);
          
          theEventsForThisWeek.push(theEventsForToday);
        });
        
        theJob.emit(theEventsForThisWeek);
      });
    },
    reduce: function(aListOfEventsForAWeek)
    {
      var theFanzoIdFinder = new FanzoIdFinder(aListOfEventsForAWeek, this, theDB);
      theFanzoIdFinder.mapEspnIdsToFanzoIds();
    },
    output: function(anInput)
    {
      console.log("output:" + JSON.stringify(anInput));
    },
    complete: function(aCallback) 
    {
      theDB.end();
      res.send("completed scraping events");
      aCallback();
    }
    
  });
  
  
  nodeio.start( theScrapeJob, 
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
                function(anError)
                {
                  if (anError)
                  {
                    console.log(anError);
                    //theDB.end();
                    res.send(anError);  
                  }
                },
                false);
  
}

var FanzoIdFinder = function(aListOfEventsForAWeek, aJob, aDB)
{
  this.myListOfEventsForAWeek = aListOfEventsForAWeek;
  this.myJob = aJob;
  this.myDB = aDB;
  
  this.myTotalNumberOfEvents = 0;
  this.myEventsCompleted = 0;
  
  this.mapEspnIdsToFanzoIds = function()
  {
    for(var i=0,j=this.myListOfEventsForAWeek.length; i<j; i++)
    {
      var theEventsForADay = this.myListOfEventsForAWeek[i];
      this.myTotalNumberOfEvents += theEventsForADay.events.length;
      for(var i=0,j=theEventsForADay.events.length; i<j; i++)
      {
        var theEvent = theEventsForADay.events[i];
        var theTeamQueryHandler = new TeamQueryHandler(theEvent, this.myDB, this.getCompleteCallback());
        theTeamQueryHandler.findTeamIds();
      };
    };
  };
  
  this.onEventComplete = function()
  {
    this.myEventsCompleted++;
    if (this.myEventsCompleted >= this.myTotalNumberOfEvents)
    {
      this.myJob.emit(this.myListOfEventsForAWeek);
    }
  };
  
  this.getCompleteCallback = function()
  {
    var theCallback = this.onEventComplete;
    var theThis = this;
    return function() { return theCallback.call(theThis)};
  }
  
}

var TeamQueryHandler = function(anEvent, aDB, aCompleteCallback)
{
  this.myEvent = anEvent;
  this.myDB = aDB;
  this.myCompleteCallback = aCompleteCallback;
  
  this.findTeamIds = function()
  {
    var theDB = this.myDB;
    var theCompleteCallback = this.myCompleteCallback;
    var theEvent = this.myEvent;
    
    theDB.query(
    {
      name: 'find team id',
      text: 'select id from teams where espn_team_id=$1',
      values: [theEvent.visitingTeamId]
    },
    function(anError, aVisitingTeamIdResult)
    {
      console.log("visiting team found at id: " + aVisitingTeamIdResult.rows[0].id);
      theEvent.visitingTeamFanzoId = aVisitingTeamIdResult.rows[0].id;
      theDB.query(
      {
        name: 'find team id',
        text: 'select id from teams where espn_team_id=$1',
        values: [theEvent.homeTeamId]
      },
      function(anError, aHomeTeamIdResult)
      {
        console.log("home team found at id: " + aHomeTeamIdResult.rows[0].id);
        theEvent.homeTeamFanzoId = aHomeTeamIdResult.rows[0].id;
        theCompleteCallback();
      });
    });
  };
}
