
var express = require('express'),
    routes = require('./routes'),
    theTeamScraper = require('./routes/scrapeTeams'),
    theTeamSaver = require('./routes/saveTeams'),
    theEventScrapers = require('./routes/scrapeEvents'),
    theLogoScrapers = require('./routes/scrapeLogos'),
    theNCAAMBBScheduleScraper = require('./routes/scrapeNCAAMBBEvents');

var app = module.exports = express();

// Configuration

app.configure(function(){
  app.set('views', __dirname + '/views');
  app.set('view engine', 'jade');
  app.use(express.bodyParser());
  app.use(express.methodOverride());
  app.use(app.router);
  app.use(express.static(__dirname + '/public'));
});

app.configure('development', function(){
  app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
});

app.configure('production', function(){
  app.use(express.errorHandler());
});

// Routes
app.get('/scrapeTeams/', theTeamScraper.scrapeTeams);
app.get('/saveTeams/', theTeamSaver.saveTeams);
app.get('/scrapeEvents/', theEventScrapers.scrapeEvents);
app.get('/scrapeLogos/', theLogoScrapers.scrapeLogos);
app.get('/scrapeNCAAMBBEvents/', theNCAAMBBScheduleScraper.scrapeEvents);
app.get('/', routes.index);

var port = 3011;
app.listen(port);
console.log("Express server listening on port %d in %s mode", port, app.settings.env);
