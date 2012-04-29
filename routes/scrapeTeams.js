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
        $("div.mod-header.colhead h4").each(function(anElement){
          theJob.emit(anElement.children[0].data);
        });
      });
    },
    output: function(anArrayOfResults) {
      anArrayOfResults.forEach( function(aResult){ 
        console.log("adding:" + aResult);
        theDB.query({
          name: 'add league',
          text: 'insert into conferences(name, league_id, created_at, updated_at) values($1, 3, now(), now())',
          values: [aResult]
        });
      } );
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
                  jsdom:false,
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
