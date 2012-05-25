var nodeio = require('node.io');
var pg = require('pg');
var theDBUrl = process.env.DATABASE_URL || "tcp://fanzo_site:fanzo_site@localhost/fanzo_site_development";


exports.saveTeams = function(req, res)
{
  var theDB = new pg.Client(theDBUrl);
  theDB.connect();
  
  var theLeague = req.param("league", "NCAAF");  
  
  var theSaveJob = new nodeio.Job({
    input: 'public/scraped_' + theLeague + '.txt',
    run:function(aConferenceString)
    {
      var theConference = JSON.parse(aConferenceString);
      console.log("grabbing addresses for: " + theConference.name);
      var theConferenceTeamAddressScraper = new ConferenceTeamAddressScraper(theConference, theLeague, this);
      theConferenceTeamAddressScraper.scrapeAddressesForTeams();
    },
    reduce: function(anArrayOfConferences)
    {
      for(var i=0,j=anArrayOfConferences.length; i<j; i++)
      {
        var theConference = anArrayOfConferences[i];
        console.log("saving teams for:" + theConference.name);
        var theConferenceTeamSaver = new ConferenceTeamSaver(theConference, theLeague, theDB, this);
        theConferenceTeamSaver.saveTeams();
      };
    },
    output: function(aConference)
    {
      console.log("finished saving: " + aConference.name);
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

var TitleLoader = function(aTeam, aDoneCallback)
{
  this.myTeam = aTeam;
  this.myDoneCallback = aDoneCallback;

  this.myCallback = function(err, $)
  {
    if (err)
    {
      console.log("Unable to get better name for " + this.myTeam.name + ": " + err);
    }
    else
    {
      this.myTeam.name = trim1($("div#sub-branding a.sub-brand-title b").text());
     
      console.log("Got better name for:" + this.myTeam.name);
      
      this.myDoneCallback();
    }
  };
  
  this.getCallback = function()
  {
    var theCallback = this.myCallback;
    var theThis = this;
    return function(err, $) { return theCallback.call(theThis, err, $)};
  };
  
}

var AddressLoader = function(aTeam, aLeague, aDoneCallback, aJob)
{
  this.myTeam = aTeam;
  this.myLeague = aLeague;
  this.myDoneCallback = aDoneCallback;
  this.myJob = aJob;
  
  this.ADDRESS_SELECTORS = 
  {
    "NCAAF" : "ul.stadium-info span.address-link a",
    "NFL" : "ul.stadium-info li.name span"
  }

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
      // console.log("attempting to get better name for:" + this.myTeam.name);
      // var theTitleLoader = new TitleLoader(this.myTeam, this.myDoneCallback);
      // this.myJob.getHtml(this.myTeam.espnUrl, theTitleLoader.getCallback());
    }
    else
    {
      this.myTeam.fullAddress = trim1($(this.ADDRESS_SELECTORS[this.myLeague]).text());
      if (this.myLeague == "NFL")
      {
        this.myTeam.fullAddress = this.myTeam.fullAddress.replace(/\r\n|\r|\n/, ',');
      }
      
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
      
      this.myTeam.name = trim1($("div#sub-branding a.sub-brand-title b").text());
      this.myTeam.stadiumName = $("ul.stadium-info li.name h3").text();
      
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

var StadiumUrlFinder = function(aTeam, aLeague, aCallback, aJob)
{
  this.myTeam = aTeam;
  this.myLeague = aLeague;
  this.myCompleteCallback = aCallback;
  this.myJob = aJob
  
  this.findStadiumUrl = function()
  {
    if (this.myLeague == "NCAAF")
    {
      var theStadiumUrl = "http://espn.go.com/college-football/team/stadium/_/id/" + this.myTeam.espnId + "/" + this.myTeam.teamSlug;
      this.myCompleteCallback(this.myTeam, theStadiumUrl);
    }
    else
    {
      this.myJob.getHtml(this.myTeam.espnUrl, this.getCallback());
    }
  }

  this.getCallback = function()
  {
    var theCallback = this.myCallback;
    var theThis = this;
    return function(err, $) { return theCallback.call(theThis, err, $)};
  }
  
  this.myCallback = function(err, $)
  {
    if (err)
    {
      console.log("****** can't get stadium url");
    }
    else
    {
      var theStadiumUrl = "";
      $("div.mod-page-tabs ul.ui-tabs li").each(function(anIndex, anElement) {
        var theListItemElement = $(anElement);
        if (theListItemElement.find("a").text() == "Stadium")
        {
          theStadiumUrl = "http://espn.go.com" + theListItemElement.find("a").attr("href");
        }
      });
      
      this.myCompleteCallback(this.myTeam, theStadiumUrl);
    }
  }  
}

var ConferenceTeamAddressScraper = function(aConference, aLeague, aJob)
{
  this.myConference = aConference;
  this.myLeague = aLeague;
  this.myJob = aJob;
  this.myNumberOfProcessedTeams = 0;
  
  this.scrapeAddressesForTeams = function()
  {
    for(var i=0,j=this.myConference.teams.length; i<j; i++)
    {
      var theTeam = this.myConference.teams[i];
      
      var theStadiumUrlFinder = new StadiumUrlFinder(theTeam, this.myLeague, this.getStadiumCallback(), this.myJob);
      theStadiumUrlFinder.findStadiumUrl();
    };
  };
  
  this.onFoundStadiumUrl = function(aTeam, aStadiumUrl)
  {
    console.log("found stadium for team: " + aTeam.name + " at: " + aStadiumUrl);   
    var theAddressLoader = new AddressLoader(aTeam, this.myLeague, this.getCompleteCallback(), this.myJob);
    this.myJob.getHtml(aStadiumUrl, theAddressLoader.getCallback());
  };
  
  this.myCompleteCallback = function()
  {
    this.myNumberOfProcessedTeams++;
    if (this.myNumberOfProcessedTeams >= this.myConference.teams.length)
    {
      this.myJob.emit(this.myConference);
    }
  };
  
  this.getStadiumCallback = function()
  {
    var theThis = this;
    var theCallback = this.onFoundStadiumUrl;
    return function() { return theCallback.apply(theThis, arguments); }
  }
  
  this.getCompleteCallback = function()
  {
    var theCallback = this.myCompleteCallback;
    var theThis = this;
    return function() { return theCallback.call(theThis)};
  }
  
}

var ConferenceTeamSaver = function(aConference, aLeague, aDB, aJob)
{
  this.myConference = aConference;
  this.myLeague = aLeague;
  this.myDB = aDB;
  this.myJob = aJob;
  this.myTeamsProcessed = 0;
  
  this.saveTeams = function()
  {
    for(var i=0,j=this.myConference.teams.length; i<j; i++)
    {
      var theTeam = this.myConference.teams[i];
      theTeam.conference_id = this.myConference.fanzoId;
      this.saveOrUpdateTeam(theTeam);
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
  
  
  this.saveOrUpdateTeam = function(aTeam)
  {
    var theProcessedCallback = this.getCompleteCallback();
    var theDB = this.myDB;
    var theLeague = this.myLeague;
    
    theDB.query(
      {
        name: "find team id",
        text: "select id, location_id from teams where name = $1",
        values: [aTeam.name]
      },
      function( anError, anIDResult )
      {
        if (anIDResult && anIDResult.rows.length > 0)
        {
          console.log("found team " + aTeam.name + " at id: " + anIDResult.rows[0].id + " with location id:" + anIDResult.rows[0].location_id);
          aTeam.fanzoId = anIDResult.rows[0].id;
          var theDBQueryText = "";
          if (theLeague == "NCAAF")
          {
            theDBQueryText = "update teams set espn_team_id=$1, espn_team_url=$2, slug=$3, short_name=$4, mascot=$5, updated_at=now() where id=$6" 
          }
          else
          {
            theDBQueryText = "update teams set espn_team_name_id=$1, espn_team_url=$2, slug=$3, short_name=$4, mascot=$5, updated_at=now() where id=$6"
          }
          theDB.query(
            {
              name: "update team data",
              text: theDBQueryText,
              values: [aTeam.espnId, aTeam.espnUrl, aTeam.teamSlug, aTeam.affiliationName, aTeam.mascot, aTeam.fanzoId]
            },
            function(anError, anUpdateResult)
            {
              console.log("updated team");
              if (anError)
              {
                console.log("Problem updating team:" + anError);
                console.log("team: " + JSON.stringify(aTeam));
              }
              theDB.query(
                {
                  name:"update location data",
                  text:"update locations set name=$1 where id=$2",
                  values: [aTeam.stadiumName, anIDResult.rows[0].location_id]
                },
                function(anError, anUpdateResult)
                {
                  console.log("updated team location");
                  if (anError)
                  {
                    console.log("Problem updating team location:" + anError);
                    console.log("team: " + JSON.stringify(aTeam));
                  }
                  theProcessedCallback();
                });
            }
          );
        }
        else
        {
          console.log("team " + aTeam.name + " not found, adding");
          theDB.query(
          {
            name: 'add location',
            text: "insert into locations(name, address1, city, state_id, postal_code, created_at, updated_at) values($1, $2, $3, $4, $5, now(), now()) returning id",
            values: [aTeam.stadiumName, aTeam.address1, aTeam.city, mapStateToId(aTeam.state), aTeam.zip]
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
            
            var theQueryText = "";
            var theValues = [aTeam.name, aTeam.conference_id, aTeam.location_id, aTeam.espnUrl, aTeam.teamSlug, aTeam.affiliationName, aTeam.mascot];
            if (theLeague == "NFL")
            {
              theQueryText = "insert into teams(name, sport_id, league_id, division_id, location_id, created_at, updated_at, espn_team_url, slug, short_name, mascot, espn_team_name_id) values($1, 1, 1, $2, $3, now(), now(), $4, $5, $6, $7, $8) returning id";
              theValues.push(aTeam.espnId);
            } 
            else
            {
              theQueryText = "insert into teams(name, sport_id, league_id, conference_id, location_id, created_at, updated_at, espn_team_url, slug, short_name, mascot, espn_team_id) values($1, 1, 2, $2, $3, now(), now(), $4, $5, $6, $7, $8) returning id";
              theValues.push(parseInt(aTeam.espnId));
            }
            
            theDB.query(
            {
              name: 'add team',
              text: theQueryText,
              values: theValues
            },
            function(anError, anAddTeamResult)
            {
              if (anError)
              {
                console.log("Problem creating team:" + anError);
                console.log("team: " + JSON.stringify(aTeam));
              }
              console.log("team created at id: " + anAddTeamResult.rows[0].id);
              aTeam.fanzoId = anAddTeamResult.rows[0].id;
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
