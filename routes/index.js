
/*
 * GET home page.
 */

exports.index = function(req, res){
	
  var pg = require('pg');
  var theDBUrl = process.env.DATABASE_URL || "tcp://cms:cms@localhost/cms";

  pg.connect( theDBUrl, function(err, client) {
    if (err)
    {
       console.log(err);
       res.render('index', { title: 'Express' + JSON.stringify(err) });
       return;
    }

    var query = client.query('SELECT * FROM sport');

    query.on('row', function(row) {
      res.render('index', { title: 'Express' + JSON.stringify(row) });
    });
  });

};



