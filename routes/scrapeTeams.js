var nodeio = require('node.io');
var pg = require('pg');
var theDBUrl = process.env.DATABASE_URL || "tcp://fanzo_site:fanzo_site@localhost/fanzo_site_development";

function convertSlugToName(aSlug)
{
  var thePieces = aSlug.split('-');
  var theResult = [];
  for(var i=0,j=thePieces.length; i<j; i++)
  {
    if(thePieces[i][0] != null)
    {
        theResult[i] = thePieces[i][0].toUpperCase() + thePieces[i].slice(1);
    }
  };
  
  return theResult.join(" ");
}

exports.scrapeTeams = function(req, res)
{
  var theDB = new pg.Client(theDBUrl);
  theDB.connect();
  var URLS = 
  {
    "NCAAF" : "http://espn.go.com/college-football/teams",
    "NFL" : "http://espn.go.com/nfl/teams"
  };
    
  var CONFERENCE_NAME_SELECTOR = 
  {
    "NCAAF" : "div.mod-header.colhead",
    "NFL" : "div.mod-header h4"
  }
  
  var URL_MATCHER = 
  {
    "NCAAF" : /http:\/\/espn.go.com\/college-football\/team\/_\/id\/(\d+)\/(.*)$/,
    "NFL" : /http:\/\/espn.go.com\/nfl\/team\/_\/name\/(\w+)\/(.*)$/
  }
  
  var FIND_CONFERENCE_SQL = 
  {
    "NCAAF" : "select id from conferences where name = $1",
    "NFL" : "select id from divisions where name = $1"
  }
    
  var ADD_CONFERENCE_SQL = 
  {
    "NCAAF" : 'insert into conferences(name, league_id, created_at, updated_at) values($1, 2, now(), now()) returning id',
    "NFL" : 'insert into divisions(name, league_id, created_at, updated_at) values($1, 1, now(), now()) returning id'
  }

  var theLeague = req.param("league", "NCAAF");
  
  console.log("Scraping teams for: " + theLeague);
  console.log("using url:" + URLS[theLeague]);
  console.log("using name selector:" + CONFERENCE_NAME_SELECTOR[theLeague]);

  var theScrapeJob = new nodeio.Job({
    input: false,
    run: function() {
      var theJob = this;
      this.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.163 Safari/535.19');
      this.getHtml(URLS[theLeague], function(err, $){
        $("div.mod-container.mod-open-list.mod-teams-list-medium").each(function(anIndex, anElement)
        {
          
          var theConferenceElement = $(anElement);
          var theConference = new Object();
          theConference.teams = new Array();
          theConference.name = theConferenceElement.find(CONFERENCE_NAME_SELECTOR[theLeague]).text();
          theConferenceElement.find("div.mod-content a.bi").each(function(aTeamIndex, aTeamElement) {
            theConference.teams[aTeamIndex] = new Object();
            theConference.teams[aTeamIndex].espnUrl = $(aTeamElement).attr("href");
            var theIdMatcher = URL_MATCHER[theLeague];
            var theMatches = theIdMatcher.exec(theConference.teams[aTeamIndex].espnUrl);
            theConference.teams[aTeamIndex].espnId = theMatches[1];
            theConference.teams[aTeamIndex].teamSlug = theMatches[2];
            if (theLeague == "NCAAF")
            {
              var theSlugName = convertSlugToName(theMatches[2]);
              theConference.teams[aTeamIndex].affiliationName = $(aTeamElement).text();
              var theRegEx = new RegExp(theConference.teams[aTeamIndex].affiliationName + " ", "i");
              if (theRegEx.test(theSlugName))
              {
                theConference.teams[aTeamIndex].mascot = theSlugName.replace( theRegEx, "");
              }
              else
              {
                var theNameParts = theSlugName.split(" ");
                
                if (theConference.teams[aTeamIndex].affiliationName.indexOf('-') > 0)
                {
                  var theFirstName = theNameParts.slice(0,2).join("-");
                  theNameParts = [theFirstName].concat(theNameParts.slice(2));
                }
                
                var theAffiliationParts = theConference.teams[aTeamIndex].affiliationName.split(" ");
                var theLastMatchIndex = 0;
                for (var i=0; i < theAffiliationParts.length; i++) 
                {
                  var theIndex = theNameParts.indexOf(theAffiliationParts[i]);
                  if (theIndex > theLastMatchIndex)
                  {
                    theLastMatchIndex = theIndex;
                  }
                };
                theConference.teams[aTeamIndex].mascot = trim1(theNameParts.slice(theLastMatchIndex+1).join(" "));
                
              }
              
              // handle special cases
              if (theConference.teams[aTeamIndex].mascot == "Redhawks"
                  && theConference.teams[aTeamIndex].affiliationName == "Miami (OH)")
              {
                theConference.teams[aTeamIndex].mascot = "RedHawks"
              }
              else if (theConference.teams[aTeamIndex].mascot == "Ragin Cajuns")
              {
                theConference.teams[aTeamIndex].mascot = "Ragin' Cajuns"
              }
              else if (theConference.teams[aTeamIndex].mascot == "Runnin Bulldogs")
              {
                theConference.teams[aTeamIndex].mascot = "Runnin' Bulldogs"
              }
              else if (theConference.teams[aTeamIndex].mascot == "Pa Red Flash")
              {
                theConference.teams[aTeamIndex].mascot = "Red Flash"
              }
              
              if (theConference.teams[aTeamIndex].mascot.length > 0)
              {
                theConference.teams[aTeamIndex].name = theConference.teams[aTeamIndex].affiliationName + " " + theConference.teams[aTeamIndex].mascot;
              }
              else
              {
                theConference.teams[aTeamIndex].name = theConference.teams[aTeamIndex].affiliationName;
              }
            }
            else
            {
              theConference.teams[aTeamIndex].name = $(aTeamElement).text();
              var theNames = theConference.teams[aTeamIndex].name.split(" ");
              theConference.teams[aTeamIndex].mascot = theNames[theNames.length - 1];
              theConference.teams[aTeamIndex].affiliationName = theNames.slice(0, theNames.length-1).join(" ");
            }
            
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
          text: FIND_CONFERENCE_SQL[theLeague],
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
              text: ADD_CONFERENCE_SQL[theLeague],
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
    output:'public/scraped_' + theLeague + '.txt',
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



function trim1 (str) 
{
    return str.replace(/^\s\s*/, '').replace(/\s\s*$/, '');
}

  
  
