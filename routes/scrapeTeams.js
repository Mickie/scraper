var nodeio = require('node.io');
var pg = require('pg');

exports.scrapeTeams = function(req, res){
  var theDBUrl = process.env.DATABASE_URL || "tcp://fanzo_site:fanzo_site@localhost/fanzo_site_development";
  
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
            theConference.teams[aTeamIndex].name = $(aTeamElement).text();
            theConference.teams[aTeamIndex].url = $(aTeamElement).attr("href");
            var theIdMatcher = /http:\/\/espn.go.com\/college-football\/team\/_\/id\/(\d+)\/(.*)$/;
            var theMatches = theIdMatcher.exec(theConference.teams[aTeamIndex].url);
            theConference.teams[aTeamIndex].id = theMatches[1];
            theConference.teams[aTeamIndex].teamUrl = theMatches[2];
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
              console.log("conference " + aResult + " not found, adding");
              theDB.query(
                {
                  name: 'add conference',
                  text: 'insert into conferences(name, league_id, created_at, updated_at) values($1, 3, now(), now())',
                  values: [aConference.name]
                },
                function(anError, anInsertResult)
                {
                  theDB.query(
                    {
                      name: "find conference id",
                      values: [aConference.name]
                    },
                    function( anError, anIDResult )
                    {
                      console.log("conference " + aConference.name + " created at id: " + anIDResult.rows[0].id);
                      aConference.fanzoId = anIDResult.rows[0].id;
                      theJob.emit(aConference);
                    });
                });
            }
          });
      } );      
    },
    output:function(aConference)
    {
      var theJob = this;
      for (var key in aConference)
      {
        console.log(key + ":" + aConference[key]);
      }
    },
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
