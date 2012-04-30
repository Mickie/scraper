var nodeio = require('node.io');
var pg = require('pg');
var theDBUrl = process.env.DATABASE_URL || "tcp://fanzo_site:fanzo_site@localhost/fanzo_site_development";

function convertSlugToName(aSlug)
{
  var thePieces = aSlug.split('-');
  var theResult = [];
  for(var i=0,j=thePieces.length; i<j; i++)
  {
    theResult[i] = thePieces[i][0].toUpperCase() + thePieces[i].slice(1);
  };
  
  return theResult.join(" ");
}

exports.scrapeTeams = function(req, res)
{
  var theDB = new pg.Client(theDBUrl);
  theDB.connect();

  var theScrapeJob = new nodeio.Job({
    input: false,
    run: function() {
      var theJob = this;
      this.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.163 Safari/535.19');
      this.getHtml("http://espn.go.com/college-football/teams", function(err, $){
        $("div.mod-container.mod-open-list.mod-teams-list-medium").each(function(anIndex, anElement)
        {
          var theConferenceElement = $(anElement);
          var theConference = new Object();
          theConference.teams = new Array();
          theConference.name = theConferenceElement.children("div.mod-header.colhead").text();
          $(anElement).find("div.mod-content a.bi").each(function(aTeamIndex, aTeamElement) {
            theConference.teams[aTeamIndex] = new Object();
            theConference.teams[aTeamIndex].url = $(aTeamElement).attr("href");
            var theIdMatcher = /http:\/\/espn.go.com\/college-football\/team\/_\/id\/(\d+)\/(.*)$/;
            var theMatches = theIdMatcher.exec(theConference.teams[aTeamIndex].url);
            theConference.teams[aTeamIndex].espnId = theMatches[1];
            theConference.teams[aTeamIndex].teamUrlName = theMatches[2];
            theConference.teams[aTeamIndex].name = convertSlugToName(theMatches[2]);
          });
          theJob.emit(theConference);
        });
      });
    },
    reduce:function(anArrayOfConferences) 
    {
      var theJob = this;
      anArrayOfConferences.forEach( function(aConference)
      {
        console.log("checking conference:" + aConference.name);
        theDB.query(
          {
            name: "find conference id",
            text: "select id from conferences where name = $1",
            values: [aConference.name]
          },
          function( anError, anIDResult )
          {
            if (anIDResult && anIDResult.rows.length > 0)
            {
              console.log("found conference " + aConference.name + " at id: " + anIDResult.rows[0].id);
              aConference.fanzoId = anIDResult.rows[0].id;
              theJob.emit(aConference);
            }
            else
            {
              console.log("conference " + aConference.name + " not found, adding");
              theDB.query(
                {
                  name: 'add conference',
                  text: 'insert into conferences(name, league_id, created_at, updated_at) values($1, 3, now(), now()) returning id',
                  values: [aConference.name]
                },
                function(anError, anInsertResult)
                {
                  console.log("conference " + aConference.name + " created at id: " + anInsertResult.rows[0].id);
                  aConference.fanzoId = anInsertResult.rows[0].id;
                  theJob.emit(aConference);
                });
            }
          });
      } );      
    },
    output:'public/scraped.txt',
    complete: function(aCallback) {
      theDB.end();
      res.send("completed");
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
                    theDB.end();
                    res.send(anError);  
                  }
                },
                false);

};



exports.saveTeams = function(req, res)
{
  var theDB = new pg.Client(theDBUrl);
  theDB.connect();
  
  var theSaveJob = new nodeio.Job({
    input: 'public/scraped.txt',
    run:function(aConferenceString)
    {
      var theConference = JSON.parse(aConferenceString);
      console.log("grabbing addresses for: " + theConference.name);
      var theConferenceTeamAddressScraper = new ConferenceTeamAddressScraper(theConference, this);
      theConferenceTeamAddressScraper.scrapeAddressesForTeams();
    },
    reduce: function(anArrayOfConferences)
    {
      for(var i=0,j=anArrayOfConferences.length; i<j; i++)
      {
        var theConference = anArrayOfConferences[i];
        console.log("saving teams for:" + theConference.name);
        var theConferenceTeamSaver = new ConferenceTeamSaver(theConference, theDB, this);
        theConferenceTeamSaver.saveTeams();
      };
    },
    output: function(aConference)
    {
      console.log("output conference = " + JSON.stringify(aConference));
    },
    complete: function(aCallback)
    {
//      theDB.end();
      res.send("save completed");
      aCallback();
    }
  }); 
  
  
  nodeio.start( theSaveJob, 
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



function trim1 (str) 
{
    return str.replace(/^\s\s*/, '').replace(/\s\s*$/, '');
}

var AddressLoader = function(aTeam, aDoneCallback)
{
  this.myTeam = aTeam;
  this.myDoneCallback = aDoneCallback;

  this.myCallback = function(err, $)
  {
    if (err)
    {
      console.log("Unable to get address info for " + this.myTeam.name + ": " + err);
      this.myTeam.address1 = "NEED TO LOOKUP";
      this.myTeam.city = '';
      this.myTeam.state = "WA";
      this.myTeam.zip = '';
      this.myDoneCallback();
    }
    else
    {
      this.myTeam.fullAddress = trim1($("ul.stadium-info span.address-link a").text());
      
      var theAddressBits = this.myTeam.fullAddress.split(',');
      if (theAddressBits.length == 3)
      {
        this.myTeam.address1 = trim1(theAddressBits[0]);
        this.myTeam.city = trim1(theAddressBits[1]);
        var theStateZipBits = trim1(theAddressBits[2]).split(' ');
        this.myTeam.state = theStateZipBits[0];
        this.myTeam.zip = theStateZipBits[1];
      }
      else if (theAddressBits.length == 2)
      {
        this.myTeam.address1 = trim1(theAddressBits[0]);
        var theCityStateZipBits = trim1(theAddressBits[1]).split(' ');
        this.myTeam.city = theCityStateZipBits[0];
        this.myTeam.state = theCityStateZipBits[1];
        this.myTeam.zip = theCityStateZipBits[2];
      }
      else
      {
        console.log("*** Unable to parse address: " + this.myTeam.fullAddress);
      }
      this.myDoneCallback();
    }
  };
  
  this.getCallback = function()
  {
    var theCallback = this.myCallback;
    var theThis = this;
    return function(err, $) { return theCallback.call(theThis, err, $)};
  }

}

var ConferenceTeamAddressScraper = function(aConference, aJob)
{
  this.myConference = aConference;
  this.myJob = aJob;
  this.myNumberOfProcessedTeams = 0;
  
  this.scrapeAddressesForTeams = function()
  {
      for(var i=0,j=this.myConference.teams.length; i<j; i++)
      {
        var theTeam = this.myConference.teams[i];
        var theStadiumUrl = "http://espn.go.com/college-football/team/stadium/_/id/" + theTeam.espnId + "/" + theTeam.teamUrlName;
        
        var theAddressLoader = new AddressLoader(theTeam, this.getCompleteCallback());
        this.myJob.getHtml(theStadiumUrl, theAddressLoader.getCallback());
      };
  };
  
  this.myCompleteCallback = function()
  {
    this.myNumberOfProcessedTeams++;
    if (this.myNumberOfProcessedTeams >= this.myConference.teams.length)
    {
      this.myJob.emit(this.myConference);
    }
  };
  
  this.getCompleteCallback = function()
  {
    var theCallback = this.myCompleteCallback;
    var theThis = this;
    return function() { return theCallback.call(theThis)};
  }
  
}

var ConferenceTeamSaver = function(aConference, aDB, aJob)
{
  this.myConference = aConference;
  this.myDB = aDB;
  this.myJob = aJob;
  this.myTeamsProcessed = 0;
  
  this.saveTeams = function()
  {
    for(var i=0,j=this.myConference.teams.length; i<j; i++)
    {
      var theTeam = this.myConference.teams[i];
      theTeam.conference_id = this.myConference.fanzoId;
      this.saveTeamIfNew(theTeam);
    };
  };
  
  this.processedTeam = function()
  {
    this.myTeamsProcessed++;
    if (this.myTeamsProcessed >= this.myConference.teams.length)
    {
      this.myJob.emit(this.myConference);
    }
  }
  
  this.getCompleteCallback = function()
  {
    var theCallback = this.processedTeam;
    var theThis = this;
    return function() { return theCallback.call(theThis)};
  }
  
  
  this.saveTeamIfNew = function(aTeam)
  {
    var theProcessedCallback = this.getCompleteCallback();
    var theDB = this.myDB;
    theDB.query(
      {
        name: "find team id",
        text: "select id from teams where name = $1",
        values: [aTeam.name]
      },
      function( anError, anIDResult )
      {
        if (anIDResult && anIDResult.rows.length > 0)
        {
          console.log("found team " + aTeam.name + " at id: " + anIDResult.rows[0].id);
          aTeam.fanzoId = anIDResult.rows[0].id;
          theProcessedCallback();
        }
        else
        {
          console.log("team " + aTeam.name + " not found, adding");
          theDB.query(
          {
            name: 'add location',
            text: "insert into locations(name, address1, city, state_id, postal_code, created_at, updated_at) values('stadium', $1, $2, $3, $4, now(), now()) returning id",
            values: [aTeam.address1, aTeam.city, mapStateToId(aTeam.state), aTeam.zip]
          },
          function(anError, anInsertResult)
          {
            if (anError)
            {
              console.log("Problem creating location:" + anError);
              console.log("team: " + JSON.stringify(aTeam));
            }
            console.log("location created at id: " + anInsertResult.rows[0].id);
            aTeam.location_id = anInsertResult.rows[0].id;
            
            theDB.query(
            {
              name: 'add team',
              text: "insert into teams(name, sport_id, league_id, conference_id, location_id, created_at, updated_at) values($1, 1, 2, $2, $3, now(), now()) returning id",
              values: [aTeam.name, aTeam.conference_id, aTeam.location_id]
            },
            function(anError, anTeamResult)
            {
              console.log("team created at id: " + anTeamResult.rows[0].id);
              aTeam.fanzoId = anTeamResult.rows[0].id;
              theProcessedCallback();
            });
          });
        }
      });
    
  };
}

function mapStateToId(anAbbreviation)
{
  return STATE_TO_ID_MAP[anAbbreviation];
}

var STATE_TO_ID_MAP =
{
  "AL":1,
  "AK":2,
  "AZ":3,
  "AR":4,
  "CA":5,
  "CO":6,
  "CT":7,
  "DE":8,
  "FL":9,
  "GA":10,
  "HI":11,
  "ID":12,
  "IL":13,
  "IN":14,
  "IA":15,
  "Iowa":15,
  "KS":16,
  "KY":17,
  "LA":18,
  "ME":19,
  "MD":20,
  "MA":21,
  "MI":22,
  "MN":23,
  "MS":24,
  "MO":25,
  "MT":26,
  "NE":27,
  "NV":28,
  "NH":29,
  "NJ":30,
  "NM":31,
  "NY":32,
  "NC":33,
  "ND":34,
  "OH":35,
  "OK":36,
  "OR":37,
  "PA":38,
  "RI":39,
  "SC":40,
  "SD":41,
  "TN":42,
  "TX":43,
  "UT":44,
  "VT":45,
  "VA":46,
  "WA":47,
  "WV":48,
  "WI":49,
  "WY":50,
  "AS":51,
  "DC":52,
  "FM":53,
  "GU":54,
  "MH":55,
  "MP":56,
  "PW":57,
  "PR":58,
  "VI":59,
  "AE":60,
  "AA":61,
  "AE":62,
  "AE":63,
  "AE":64,
  "AP":65
};
  
  
