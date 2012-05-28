var nodeio = require('node.io');
var pg = require('pg');
var theDBUrl = process.env.DATABASE_URL || "tcp://fanzo_site:fanzo_site@localhost/fanzo_site_development";


exports.scrapeLogos = function(req, res)
{
  var theDB = new pg.Client(theDBUrl);
  theDB.connect();
  var theLeague = req.param("league", "NCAAF");

  var LOAD_TEAMS_SQL = 
  {
    "NCAAF" : "select id, espn_team_id as espn_id, slug from teams where league_id=2",
    "NFL" : "select id, espn_team_name_id as espn_id, slug from teams where league_id=1"
  }

  console.log("scraping data for:" + theLeague);
  
  var theSaveJob = new nodeio.Job({
    input: false,
    run:function()
    {
        var theJob = this;
        theDB.query(
        {
          name: 'get espn data for every team',
          text: LOAD_TEAMS_SQL[theLeague]
        },
        function(anError, aQueryResult)
        {
          console.log("got data: " + aQueryResult.rows.length + " rows");
          for(var i=0,j=aQueryResult.rows.length; i<j; i++)
          {
            theTeam = new Object()
            theTeam.fanzoId = aQueryResult.rows[i].id
            theTeam.espnId = aQueryResult.rows[i].espn_id;
            theTeam.teamSlug = aQueryResult.rows[i].slug;
            theJob.emit(theTeam);
          };
        });
    },
    reduce: function(aTeamList)
    {
      for (var i=0; i < aTeamList.length; i++) 
      {
        theTeam = aTeamList[i];
        var theLogoSaver = new LogoSaver(theTeam, theLeague, this);
        theLogoSaver.saveLogos();
      };
    },
    output: function(aTeam)
    {
      console.log("completed saving team:" + aTeam.teamSlug)
    },
    complete: function(aCallback)
    {
      theDB.end();
      res.send("image save completed");
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
                    theDB.end();
                    res.send(anError);  
                  }
                },
                false);
   
}

var LogoSaver = function(aTeam, aLeague, aJob)
{
  this.myTeam = aTeam;
  this.myJob = aJob;
  this.myLeague = aLeague == "NCAAF" ? "ncaa" : "nfl";
  
  
  this.mySmallLogoUrl = "http://a.espncdn.com/i/teamlogos/" + this.myLeague + "/sml/trans/" + aTeam.espnId + ".gif";
  this.mySmallLogoPath = "public/images/logos/" + aTeam.teamSlug + "_s.gif";
  this.myMediumLogoUrl = "http://a.espncdn.com/i/teamlogos/" + this.myLeague + "/med/trans/" + aTeam.espnId + ".gif";
  this.myMediumLogoPath = "public/images/logos/" + aTeam.teamSlug + "_m.gif";
  this.myLargeLogoUrl = "http://a.espncdn.com/i/teamlogos/" + this.myLeague + "/lrg/trans/" + aTeam.espnId + ".gif";
  this.myLargeLogoPath = "public/images/logos/" + aTeam.teamSlug + "_l.gif";
      
  this.saveLogos = function()
  {
    var theJob = this.myJob;
    var theLogoSaver = this;
    var theUserAgent = "'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.163 Safari/535.19'";

    var theSaveSmallCommand = "curl --create-dirs -L -A " + theUserAgent + " -o " + theLogoSaver.mySmallLogoPath + " " + theLogoSaver.mySmallLogoUrl;
    var theSaveMediumCommand = "curl --create-dirs -L -A " + theUserAgent + " -o " + theLogoSaver.myMediumLogoPath + " " + theLogoSaver.myMediumLogoUrl;
    var theSaveLargeCommand = "curl --create-dirs -L -A " + theUserAgent + " -o " + theLogoSaver.myLargeLogoPath + " " + theLogoSaver.myLargeLogoUrl;

    theJob.exec(theSaveSmallCommand, function(err, stdout)
    {
      theJob.exec(theSaveMediumCommand, function(err, stdout)
      {
        theJob.exec(theSaveLargeCommand, function(err, stdout)
        {
          theJob.emit(theLogoSaver.myTeam);
        })

      })
    })


  };
  

}
